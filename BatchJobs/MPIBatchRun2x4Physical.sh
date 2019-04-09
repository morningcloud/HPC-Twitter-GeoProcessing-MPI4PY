#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --partition=physical
#SBATCH --ntasks-per-node=4
module load Python/3.4.3-goolf-2015a
mpiexec python finalscript.py

