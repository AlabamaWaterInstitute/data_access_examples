# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.9 as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
RUN mkdir -p ${FUNCTION_DIR}/ngen_forcing
RUN mkdir -p ${FUNCTION_DIR}/subsetting
RUN mkdir -p ${FUNCTION_DIR}/nwm_filenames
COPY ./ngen_forcing ${FUNCTION_DIR}/ngen_forcing
COPY ./subsetting ${FUNCTION_DIR}/subsetting
COPY ./nwm_filenames ${FUNCTION_DIR}/nwm_filenames
COPY requirements.txt ${FUNCTION_DIR}
COPY lambda_function.py ${FUNCTION_DIR}

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

# Use a slim version of the base Python image to reduce the final image size
FROM python:3.9-slim

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

RUN pip install -r "${FUNCTION_DIR}/requirements.txt" --target ${FUNCTION_DIR} 
RUN pip install --upgrade google-api-python-client
RUN pip install --upgrade google-cloud-storage

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# # Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_function.handler" ]
