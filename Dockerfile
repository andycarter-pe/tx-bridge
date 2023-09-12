# In the folder containing this Dockerfile, run the following command
# docker build -t civileng127/tx-bridge:20230911 .
#
# to ensure that the git clone is not cached
# docker build --no-cache -t civileng127/tx-bridge:20230911c .

# Use continuumio/miniconda3 as a parent image
FROM continuumio/miniconda3

# Set the working directory in the container
WORKDIR /app

# Update Conda
RUN conda update -n base -c defaults conda

# Create a directory named "tx-bridge"
RUN mkdir tx-bridge

# Clone the Git repository into the /tx-bridge directory (use hpc_run branch)
RUN git clone -b hpc_run https://github.com/andycarter-pe/tx-bridge.git /tx-bridge

# Copy the requirements.txt file into the container at /app
COPY requirements.txt .

# Install packages from requirements.txt in the base environment from Conda Forge
RUN conda config --add channels conda-forge && \
    conda install --file requirements.txt && \
    conda clean --all -f -y
	
# Install packages using pip
RUN pip install pylas netCDF4

RUN apt update
RUN apt install nano