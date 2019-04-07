#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --partition=cloud
#SBATCH --ntasks-per-node=8
module load Python/3.4.3-goolf-2015a
mpiexec -np 8 python finalscript.py

