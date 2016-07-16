#!/usr/bin/python

import datetime, math, os, re, shutil, subprocess, sys

_COPY_FILES = ( 'flash4', 'LS220_240r_140t_50y_analmu_20120628_SVNr28.h5', 'm25q6.short' )
_PAR_FILE = 'flash.par'

_PART_SIZE = 512
_MINIMUM_SUBBLOCK_SIZE = 32
_SHAPES = {   32: ( 2, 2, 2, 2, 2 ),
              64: ( 2, 2, 4, 2, 2 ),
             128: ( 2, 2, 4, 4, 2 ),
             256: ( 4, 2, 4, 4, 2 ),
             512: ( 4, 4, 4, 4, 2 ) }

_RUN_TIME = 42000
_TS_PATTERN = r'(?:\[ )?(?P<month>\d\d)-(?P<day>\d\d)-(?P<year>\d{4})\s+(?P<hour>\d\d):(?P<minute>\d\d)[:.](?P<second>\d\d)(?P<fraction_second>\.\d+)?(?: \])?'


def log_msg(out, *msgs):
  now = datetime.datetime.now()
  print >>out, '[%s]' % now.replace(microsecond=0).isoformat(' '),
  for msg in msgs[:-1]:
    print >>out, msg,
  print >>out, msgs[-1]
  out.flush()


def create_datetime(fraction_second=None, **kwargs):
  """
  Create a datetime object from a groupdict like the one created by the
  regular expression '_TS_PATTERN', defined above.  Extra keyword arguments
  are conveniently ignored.
  """
  dateargs = {}
  for key in ( 'year', 'month', 'day', 'hour', 'minute', 'second' ):
    if key in kwargs and kwargs[key] is not None:
      dateargs[key] = int(kwargs[key])
    else:
      dateargs[key] = 0
  if fraction_second:
    dateargs['microsecond'] = int(1e6 * float(fraction_second))
  else:
    dateargs['microsecond'] = 0
  return datetime.datetime(**dateargs)


def scan_predecessor(predecessor_dir, sim_name):
  log_name = os.path.join(predecessor_dir, sim_name + '.log')
  start_time = None
  last_time = None
  last_simtime = None
  last_timestep = None
  checkpoint_file = None
  checkpoint_number = 0
  plot_number = 0
  next_plot_number = None
  particle_number = 0
  next_particle_number = None
  crashed = True
  logfile = open(log_name)
  for full_line in logfile:
    line = full_line.strip()
    tmatch = re.search(_TS_PATTERN, line)
    if tmatch:
      last_time = create_datetime(**tmatch.groupdict())

    tmatch = re.search(r'FLASH log file:\s+' + _TS_PATTERN, line)
    if tmatch:
      start_time = create_datetime(**tmatch.groupdict())

    tmatch = re.search(r' step: n=(\d+) t=(\S+) dt=(\S+)', line)
    if tmatch:
      last_timestep = int(tmatch.group(1))
      last_simtime = float(tmatch.group(2)) + float(tmatch.group(3))

    tmatch = re.search(r'\[IO[ _]write(\w+)\] close:(?: type=\w+)? ' \
                       r'name=(?:[^:]*:)?([^:]+_([0-9]{4}))', line)
    if tmatch:
      filetype = tmatch.group(1).lower()
      if filetype == 'particles':
        particle_number = int(tmatch.group(3))
      elif filetype == 'plotfile':
        if '_forced_' not in tmatch.group(2):
          plot_number = int(tmatch.group(3))
      elif filetype == 'checkpoint':
        checkpoint_file = tmatch.group(2)
        checkpoint_number = int(tmatch.group(3))
        next_plot_number = plot_number + 1
        next_particle_number = particle_number + 1

    tmatch = re.search(_TS_PATTERN + \
                       r'\s+LOGFILE_END: FLASH run complete.', line)
    if tmatch:
      crashed = False

#  if crashed:
#    log_msg(trace, '*FATAL* Crashed predecessor in %s' % predecessor_dir)
#    return

#  if last_simtime >= 1.0:
#    log_msg(trace, '*FATAL* Simulation completed in predecessor in %s' % predecessor_dir)
#    return

#  if last_time - start_time < datetime.timedelta(seconds=_RUN_TIME):
#    log_msg(trace, '*FATAL* Early exit by predecessor in %s' % predecessor_dir)
#    return

  return ( checkpoint_number, next_plot_number, next_particle_number )


def compute_corner_coords(block, shape):
  dims = []
  ( origin, extent ) = block.split('-')[1:3]
  origin = [ int(o, 16) for o in origin ]
  for i in range(5):
    dim = int(extent[i], 16) - origin[i] + 1;
    dims.append(dim)
  corners = []
  for i in xrange(0, dims[0], shape[0]):
    for j in xrange(0, dims[1], shape[1]):
      for k in xrange(0, dims[2], shape[2]):
        for l in xrange(0, dims[3], shape[3]):
          for m in xrange(0, dims[4], shape[4]):
            corners.append('%x%x%x%x%x' % \
                           ( i + origin[0], j + origin[1], k + origin[2],
                             l + origin[3], m + origin[4] ))
  return corners


def main(argv):
  # Open trace file
  job_id = os.environ['COBALT_JOBID']
  trace = open(job_id + '.output', 'w')

  total_nodes = int(os.environ['COBALT_JOBSIZE'])
  log_msg(trace, 'Total nodes: %d' % total_nodes)

  # Determine sub-block size
  subblock_nodes = _MINIMUM_SUBBLOCK_SIZE
  if argv[0] == '-n':
    subblock_nodes = int(argv[1])
    del argv[:2]
  log2 = math.log(subblock_nodes, 2)
  if subblock_nodes < _MINIMUM_SUBBLOCK_SIZE or \
     subblock_nodes > _PART_SIZE or \
     log2 != math.floor(log2):
    log_msg(trace, '*FATAL* Invalid sub-block size %d.' % subblock_nodes)
    log_msg(trace, '*FATAL* Sub-block size must be a power of 2 between %d and %d.' % \
            ( _MINIMUM_SUBBLOCK_SIZE, _PART_SIZE ))
    trace.close()
    return
  log_msg(trace, 'Sub-block size: %d' % subblock_nodes)

  # Verify number of separate jobs
  job_count = len(argv)
  if job_count > total_nodes / subblock_nodes:
    log_msg(trace, '*FATAL* Not enough nodes to run %d %d-node jobs (%d required, only %d available.' % ( job_count, subblock_nodes, job_count * subblock_nodes, total_nodes ))
    trace.close()
    return

  # Configure restarts
  bailout = False
  for relative_exe_path in argv:
    exe_path = os.path.abspath(relative_exe_path)
    ( run_dir, executable ) = os.path.split(exe_path)
    run_number = int(run_dir[-3:])
    if run_number == 1:
      if not os.path.isfile(exe_path):
        log_msg(trace, '*FATAL* Executable for initial run not found (%s)' % exe_path)
        bailout = True
      continue

    predecessor_dir = '%s%03d' % ( run_dir[:-3], run_number - 1 )
    sim_name = os.path.basename(os.path.dirname(predecessor_dir))
    numbers = scan_predecessor(predecessor_dir, sim_name)
    if numbers is None:
      bailout = True
      continue

    ( checkpoint_number, next_plot_number, next_particle_number ) = numbers

    if not os.path.isdir(run_dir):
      try:
        os.makedirs(run_dir, 0770)
        outdir = os.path.join(run_dir,'output/')
        os.makedirs(outdir, 0770)
      except Exception, e:
        log_msg(trace, '*FATAL* Error creating directory %s' % run_dir)
        log_msg(trace, e)
        bailout = True
        continue

    try:
      for f in _COPY_FILES:
        shutil.copy2(os.path.join(predecessor_dir, f), run_dir)
    except Exception, e:
      log_msg(trace, '*FATAL* Error copying file %s' % f)
      log_msg(trace, e)
      bailout = True
      continue

    par_path = os.path.join(predecessor_dir, _PAR_FILE)
    try:
      old_par_file = open(par_path)
    except Exception, e:
      log_msg(trace, '*FATAL* Error opening %s for reading' % par_path)
      log_msg(trace, e)
      bailout = True
      continue

    par_path = os.path.join(run_dir, _PAR_FILE)
    try:
      par_file = open(par_path, 'w')
    except Exception, e:
      log_msg(trace, '*FATAL* Error opening %s for reading' % par_path)
      log_msg(trace, e)
      bailout = True
      continue

    for full_line in old_par_file:
      line = full_line.strip()
      tmatch = re.search(r'restart\s*=\s*.false.', line, re.I)
      if tmatch:
        par_file.write(full_line.replace('false', 'true'))
        continue
      tmatch = re.search(r'checkpointFileNumber\s*=\s*(\d+)', line, re.I)
      if tmatch:
        par_file.write(full_line.replace(tmatch.group(1),
                                        '%d' % checkpoint_number))
        continue
      tmatch = re.search(r'plotFileNumber\s*=\s*(\d+)', line, re.I)
      if tmatch:
        par_file.write(full_line.replace(tmatch.group(1),
                                         '%d' % next_plot_number))
        continue
      tmatch = re.search(r'particleFileNumber\s*=\s*(\d+)', line, re.I)
      if tmatch:
        par_file.write(full_line.replace(tmatch.group(1),
                                         '%d' % next_particle_number))
        continue
      par_file.write(full_line)

    par_file.write(full_line)
    par_file.close()
    old_par_file.close()

    cpfile = 'output/%s_hdf5_chk_%04d' % ( sim_name, checkpoint_number )
    os.symlink(os.path.join(predecessor_dir, cpfile),
               os.path.join(run_dir, cpfile))

  if bailout:
    trace.close()
    return

  # Get the bootable blocks.
  log_msg(trace, 'Getting bootable blocks.')
  trace.flush()
  args = [ 'get-bootable-blocks', '--size', '%d' % _PART_SIZE, os.environ['COBALT_PARTNAME'] ]
  proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  proc.wait()

  blocks = []
  procs = []
  for full_line in proc.stdout:
    block = full_line.strip()
    if block:
      procs.append(subprocess.Popen([ 'boot-block', '--block', block ]))
      blocks.append(block)

  if not blocks:
    log_msg(trace, '*FATAL* No bootable blocks!')
    trace.close()
    return

  log_msg(trace, 'Computing sub-block corners.')
  trace.flush()
  block_corners = []
  for block in blocks:
    for coord in compute_corner_coords(block, _SHAPES[subblock_nodes]):
      args = [ '/soft/cobalt/bgq_hardware_mapper/coord2hardware', coord ]
      proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
      proc.wait()
      corner = proc.stdout.read().strip()
      block_corners.append(( block, corner ))

  for proc in procs:
    proc.wait()

  log_msg(trace, 'Launching.')
  trace.flush()
  procs = []
  for ( exe_path, ( block, corner ) ) in zip(argv, block_corners):
    ( cwd, exe_file ) = os.path.split(os.path.abspath(exe_path))
    args = [ 'runjob', '--cwd', cwd, '--block', block, '--corner', corner, '--shape', '%dx%dx%dx%dx%d' % _SHAPES[subblock_nodes],
             '--ranks-per-node', '8',
             '--envs', 'BG_THREADLAYOUT=2',
             '--envs', 'BG_SHAREDMEMSIZE=32',
             '--envs', 'BG_COREDUMPONERROR=1',
             '--envs', 'OMP_NUM_THREADS=8',
             '--envs', 'OMP_STACKSIZE=16M',
             '--envs', 'L1P_POLICY=std',
             '--envs', 'PAMID_VERBOSE=1',
             ':', exe_file ]
    log_msg(trace, ' '.join(args))
    trace.flush()
    sim_name = os.path.basename(os.path.dirname(os.path.dirname(exe_path)))
    procs.append(subprocess.Popen(args, stdout=open(os.path.join(cwd, sim_name + '.output'), 'w'), stderr=open(os.path.join(cwd, sim_name + '.error'), 'w')))

  for proc in procs:
    proc.wait()

  log_msg(trace, 'Freeing blocks')
  for block in blocks:
    proc = subprocess.Popen([ 'boot-block', '--block', block, '--free' ])
    procs.append(proc)

  for proc in procs:
    proc.wait()

  log_msg(trace, 'Done!')
  trace.close()


if __name__ == '__main__':
  main(sys.argv[1:])
