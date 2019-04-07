#!/bin/bash
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --partition=physical
#SBATCH --ntasks-per-node=1
module load Python/3.4.3-goolf-2015a
mpiexec -np 1 python finalscript.py

