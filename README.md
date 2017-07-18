# SIMpliPy: Simplify your simulations with Python!

A collection of Python classes and scripts for managing scientific simulation

Take a look at [Sumatra](https://pythonhosted.org/Sumatra/introduction.html).

## Development Roadmap

**1. Automatic restarts.** This could be accomplished by the runscript calling a restart script after job completion. This _may_ be tricky, though, since the runjob script will be running on compute nodes and not on a head node. This is why we may need a job "monitoring" process that waits on the head node for jobs to be completed, then issues the restart setup. The configuration of the job restart could be accomplished in the runjob script. Just need to submit a string of dependent jobs that first call the restart script.

**2. Automatic syncing.** Automatically send metadate and .dat files to a specified remote. Again, could be done within runjob script, but the head nodes run a stripped down OS, so might not be possible...  Need a monitoring process.

**3. Automatic archiving.** Simple `rsync` calls will do, at first.