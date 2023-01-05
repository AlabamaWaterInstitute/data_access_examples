#!/bin/python
##############################################################################
#
#  Originally developed at the
#  United States Department of Commerce
#  NOAA (National Oceanic and Atmospheric Administration)
#  National Weather Service
#  Office of Water Prediction
#
#  Author:
#      Anders Nilsson, UCAR (created)
#
#
#  Further development by scientists at CIROH,
#  The Cooperative Institute for Research to Operations in Hydrology
#
##############################################################################

"""
This function generates a single timeseries from a list of files. The
variable associated with time is an additional argument
"""

# Global system imports
from argparse import ArgumentParser
import logging
import math
import os
from os.path import basename
import re
import socket
import sys
import time
import traceback

# Third-party imports
import numpy
from netCDF4 import Dataset, num2date, date2num

# Global default constants

__version__ = "1.0"

# Format for logged messages
DEFAULT_LOG_FORMAT = (
    "%(asctime)s "
    + socket.gethostname()
    + " %(filename)s[%(process)d]: %(levelname)s: %(message)s"
)

# Format for the dates used in logged messages
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %Z"

#  Level of messages that are written to the log
DEFAULT_LEVEL = logging.INFO

# Global logger instance
logger = logging.getLogger()

#######################################################################
#
#  format_current_exception
#
#######################################################################


def format_current_exception():
    """Reformat the current exception into a suitable log message

    This method reformats the current exception message including the
    stack trace into a one-line message suitable for log files.

    Args:
        None

    Exceptions:
        None
    """
    # Get current exception information
    (exception_type, exception_value, exception_traceback) = sys.exc_info()

    # Get traceback entries
    traceback_entries = traceback.extract_tb(exception_traceback)

    # Construct one-line message
    message = ""
    for entry in traceback_entries:
        message += "%s[%s[line %ld]]: " % (basename(entry[0]), entry[2], entry[1])

    # Replace any newlines in the exception text
    single_line = re.sub(" *\r?\n *", " : ", str(exception_value))

    # Return created message
    return message + single_line


############################################################################
#
#  setup_arguments
#
############################################################################
def setup_arguments():
    """Configure command line parser

    This method configures the command line argument parser and help message.

    Args:
        None

    Returns:
        A namespace containing all set configuration options

    Exceptions:
        None
    """

    # Initialize constants
    program_description = ""
    epilogue = ""

    # Get command line arguments
    parser = ArgumentParser(description=program_description, epilog=epilogue)

    # Output file
    parser.add_argument(
        "output_file",
        action="store",
        type=str,
        help="Output file that contains the time series",
    )

    # Input files
    parser.add_argument(
        "input_files",
        metavar="INPUT",
        type=str,
        nargs="+",
        help="Input files, with the first entry used as a " "template",
    )

    # Data variable(s)
    parser.add_argument(
        "-d",
        "--data_variable",
        dest="data_variables",
        action="append",
        type=str,
        help="Specify a variable to concatenate by time. "
        "May be specified more than once.",
        metavar="VARIABLE",
    )

    # Time variable option
    parser.add_argument(
        "-t",
        "--time_variable",
        dest="time_variable",
        action="store",
        type=str,
        default="time",
        help="Specify the name of the time " 'variable, otherwise defaults to "time"',
        metavar="VARIABLE",
    )

    # Reference time variable
    parser.add_argument(
        "-r",
        "--reference_time_variable",
        dest="reference_time_variable",
        action="store",
        type=str,
        default="reference_time",
        help="Specify the name of the reference time variable,"
        ' otherwise defaults to "reference_time"',
        metavar="VARIABLE",
    )

    # Skip files with missing variables option
    parser.add_argument(
        "-sm",
        "--skip_missing",
        dest="skip_missing",
        action="store_true",
        default=False,
        help="Skip input files that are missing required "
        "variables. Default is to error and stop if "
        "one is missing.",
    )

    # Global attributes to delete
    parser.add_argument(
        "-g",
        "--global_delete",
        dest="removed_global_attributes",
        action="append",
        type=str,
        help="Specify a global attribute that will not be "
        "copied to the output file. May be specified "
        "more than once.",
        metavar="ATTRIBUTE",
    )

    # Chunk size
    parser.add_argument(
        "-c",
        "--chunk_size",
        dest="chunk_size",
        action="store",
        type=int,
        default=5120,
        help="Specify the number of rows within a chunk" ". Defaults to 5120.",
        metavar="ROWS",
    )
    # Max memory in bytes
    parser.add_argument(
        "-m",
        "--max_memory",
        dest="max_memory",
        action="store",
        type=int,
        default=1e9,
        help="Specify the approximate maximum memory used "
        "(in bytes). Defaults to 1GB (1e9)",
        metavar="BYTES",
    )

    # Get version information
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + str(__version__),
        help="Display version number",
    )

    # Log message filtering by level
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        help="Display all info messages",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        dest="quiet",
        help="Display only error messages",
    )

    # Additional logging options
    parser.add_argument(
        "-s",
        "--syslog",
        dest="log_facility",
        action="store",
        type=str,
        help="Route logging messages to syslog facility " "FACILITY",
        metavar="FACILITY",
    )
    parser.add_argument(
        "-l",
        "--log",
        dest="log_file",
        action="store",
        type=str,
        help="Route logging messages to log file LOGFILE",
        metavar="LOGFILE",
    )

    # This can exit if the help option is requested
    return parser.parse_args()


############################################################################
#
#  init_logging
#
############################################################################
def init_logging():
    """Initialize logging setup

    This method intializes logging for the program.

    Args:
        None

    Returns:
        None

    Exceptions:
        None
    """
    # Set up logging
    logging.basicConfig(format=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)


############################################################################
#
#  set_logging_options
#
############################################################################
def set_logging_options(options):
    """Set logging options

    This method toggles any logging options set from the command line.

    Args:
        options (namespace argparse return) A namespace containing set
            variables from the command line. This is returned from the
            parse_args() method.

    Returns:
        None

    Exceptions:
        None
    """

    # Verbose logging?
    if options.verbose:
        log_level = logging.DEBUG
    elif options.quiet:
        log_level = logging.WARNING
    else:
        log_level = DEFAULT_LEVEL

    logger.setLevel(log_level)

    # Set up other logging destinations
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    if options.log_facility:
        syshandler = logging.handlers.SysLogHandler(facility=options.log_facility)
        syshandler.setFormatter(formatter)
        logger.addHandler(syshandler)
    if options.log_file:
        filehandler = logging.FileHandler(options.log_file)
        filehandler.setFormatter(formatter)
        logger.addHandler(filehandler)


############################################################################
#
#  Validate common dimensions
#
############################################################################
def validate_common_dimensions(input_files, skip_missing):
    """Ensure common dimensions among input files.

    This routine ensures that within the given list of input files, the
    dimensions match.

    Args:
        input_files (list of strings): A list of input NetCDF files
        skip_missing (Boolean): Whether or not to skip files that
            have the wrong dimensions, or just error

    Returns:
        A list of validated input NetCDF file paths

    Exceptions:
        ValueError if any of the dimensions do not match
    """

    logger.info("Validating common dimensions")
    new_input_files = []

    # Adding file test for clearer error messages
    if not os.path.isfile(input_files[0]):
        raise ValueError("Input file %s does not exist" % input_files[0])

    with Dataset(input_files[0], "r") as archetype:
        archetype_list = archetype.dimensions

        for filename in input_files:

            try:
                # Adding file test for clearer error messages
                if not os.path.isfile(filename):
                    raise ValueError("Input file %s does not exist" % filename)

                with Dataset(filename, "r") as input_file:
                    other_dimensions = input_file.dimensions

                    # Compare dimensions between the two files
                    for dimension_name in archetype_list.keys():

                        if (dimension_name not in other_dimensions) or (
                            len(archetype_list[dimension_name])
                            != len(other_dimensions[dimension_name])
                        ):
                            raise ValueError(
                                "The NetCDF file {0} does not "
                                "have the same dimensions as {1} "
                                "({2}).".format(
                                    filename, input_files[0], dimension_name
                                )
                            )
                        # Otherwise, dimension present and matches

                # Input file is good
                new_input_files.append(filename)

            except BaseException:
                if skip_missing:
                    # Display error and continue
                    logger.error(format_current_exception())
                else:
                    # Error
                    raise

    # Update input list if necessary
    if len(input_files) != len(new_input_files):
        logger.info(
            "Updating input file list with %d subtraction(s)",
            len(input_files) - len(new_input_files),
        )
        input_files = new_input_files

    logger.info("Done validating common dimensions")
    return input_files


############################################################################
#
#  Validate variables
#
############################################################################
def validate_variables(input_files, variable_names, skip_missing):
    """Ensure specified variables present among input files.

    This routine ensures that the input files have all specified variables
    present.

    Args:
        input_files (list of strings): A list of input NetCDF files
        variable_names (list of strings): A list of variable names
        skip_missing (Boolean): Whether or not to skip files that are
            missing a variable. When this is True, a file that is missing
            the specified variable(s) will be removed from the input file
            list.

    Returns:
        A list of validated input NetCDF file paths

    Exceptions:
        ValueError if skip_missing is False and a file is missing a
            specified variable
    """

    # Initialize
    logger.info("Validating variable presence")
    new_input_files = []

    for filename in input_files:

        try:
            # Adding file test for clearer error messages
            if not os.path.isfile(filename):
                raise ValueError("Input file %s does not exist" % filename)

            with Dataset(filename, "r") as input_file:

                for variable_name in variable_names:
                    if variable_name not in input_file.variables:
                        raise ValueError(
                            "Variable {0} not present in "
                            "{1}".format(variable_name, filename)
                        )

            # Input file is good
            new_input_files.append(filename)

        except BaseException:
            if skip_missing:
                # Display error and continue
                logger.error(format_current_exception())
            else:
                # Error
                raise

    # Update input list if necessary
    if len(input_files) != len(new_input_files):
        logger.info(
            "Updating input file list with %d subtraction(s)",
            len(input_files) - len(new_input_files),
        )
        input_files = new_input_files

    logger.info("Done validating variable presence")
    return input_files


############################################################################
#
#  Copy dimensions
#
###########################################################################
def copy_dimensions(input_file, output_file, dimension_names, new_size):
    """Copy dimensions from one NetCDF file to another

    This routine copies dimensions from one NetCDF file to another, with
    the exception that any time dimensions are set to a specified number.

    Args:
        input_file (netCDF namespace Dataset): NetCDF file opened for reading
        output_file (netCDF namespace Dataset): NetCDF file opened for writing
        dimension_names (list of strings): A list of dimensions whose lengths
            will be set to a new size instead of copying the original value.
        new_size (integer): The new length of the specified dimensions

    Returns:
        None

    Exceptions:
        None
    """

    for (dimension_name, dimension) in input_file.dimensions.items():
        if dimension_name in dimension_names:
            dimension_length = new_size
        elif dimension.isunlimited():
            dimension_length = None
        else:
            dimension_length = len(dimension)

        output_file.createDimension(dimension_name, dimension_length)


###########################################################################
#
#  Copy variable attributes
#
##########################################################################
def copy_attributes(input_variable, output_variable, removed_attributes=None):
    """Copy attributes from one variable to another

    This method creates a dictionary of attributes for greater python
    version compatibility. This can also work on root Datasets, copying the
    global attributes.

    Args:
        input_variable (netCDF namespace variable): variable to copy from
        output_variable (netCDF namespace variable): variable to copy to
        removed_attributes (list of strings): A list of attribute names to
            not copy to the new variable.
    Returns:
        None

    Exceptions:
        None
    """

    new_attributes = {}

    for key in input_variable.ncattrs():
        # Filter out unwanted attributes
        if not removed_attributes or key not in removed_attributes:
            new_attributes[key] = input_variable.getncattr(key)

    output_variable.setncatts(new_attributes)


############################################################################
#
#  Copy coordinate variables
#
############################################################################
def copy_coordinate_variables(input_file, output_file, excluded_variables):
    """Copy coordinate variables from one NetCDF file to another

    This routine copies coordinate variables from one NetCDF file to another,
    excluding any on a specified list.

    Args:
        input_file (netCDF namespace Dataset): NetCDF file opened for reading
        output_file (netCDF namespace Dataset): NetCDF file opened for writing
        excluded_variables (list of strings): A list of coordinate variable
            names that will not be copied over to the new NetCDF file.

    Returns:
        None

    Exceptions:
        None
    """

    all_variables = set(input_file.variables)
    all_dimensions = set(input_file.dimensions)
    # Coordinate variables are variables that have dimensions with the same
    # name
    coordinate_variables = all_variables & all_dimensions

    # Skip specified variables
    for excluded_variable in excluded_variables:
        if excluded_variable in coordinate_variables:
            coordinate_variables.remove(excluded_variable)

    for variable_name in coordinate_variables:
        logger.info("Creating coordinate variable %s", variable_name)
        coordinate = input_file.variables[variable_name]

        # Create the new variable. Enabling compression with default chunk size
        output_coordinate = output_file.createVariable(
            variable_name,
            coordinate.dtype,
            (variable_name,),
            zlib=True,
            complevel=4,
            shuffle=True,
        )

        # Copy any attributes
        copy_attributes(coordinate, output_coordinate)

        # Copy any values
        output_coordinate[:] = coordinate[:]


########################################################################
#
#  create processing chunk ranges
#
#######################################################################
def create_processing_chunks(number_files, number_rows, chunk_rows, max_memory):
    """Create a list of processing ranges

    This divides up the primary dimension into processing ranges in
    order to conserve memory, where each range is either a multiple of the
    output chunk size, or the entire file.

    Args:
        number_files (integer): The number of files that are being used,
        number_rows (integer): The size of the input primary data dimension
        chunk_rows (integer): The number of rows in each netCDF file chunk
        max_memory (integer): The approximage maximum number of bytes of
            memory that the function should use.

    Returns:
        A list of ranges that span the specified number of rows

    Exceptions:
        None
    """

    # Initialize
    processing_chunks = []
    previous_row = 0

    # This is a best guess on function internals after folowing function
    # behavior with "top".
    bytes_per_value = 8 * 5

    # Want processing chunk rows to be multiples of file storage chunk rows,
    # if possible.
    batch_rows = min(
        int(
            math.ceil(
                min(max_memory, float(number_rows) * number_files * bytes_per_value)
                / (bytes_per_value * number_files)
                / chunk_rows
            )
        )
        * chunk_rows,
        number_rows,
    )

    # Have to make sure is not one single row (range does not like step 0)
    if batch_rows != 0:
        for row in range(batch_rows, number_rows - 1, batch_rows):
            processing_chunks.append([previous_row, previous_row + batch_rows])
            previous_row = row

    processing_chunks.append([previous_row, number_rows])

    return processing_chunks


############################################################################
#
#  create time series variable
#
############################################################################
def create_time_series_variable(
    input_file,
    output_file,
    variable_name,
    time_variable,
    reference_time_variable,
    input_filepaths,
    chunk_rows,
    max_memory,
):
    """Initialize the output time series variable

    This routine initializes the output time series variable with attributes,
    dimensions, and chunking information copied from the input file.

    Args:
        input_file (netCDF namespace Dataset): NetCDF file opened for reading
        output_file (netCDF namespace Dataset): NetCDF file opened for writing
        variable_name (string): The name of the time series data variable
        time_variable (string): The name of the time variable
        reference_time_variable (string): The name of the reference time
            variable
        input_file_paths (list of strings): A list of input NetCDF files
        chunk_rows (integer): The number of rows that span a chunk
        max_memory (integer): The approximate maximum number of bytes of
            memory that the function should use.

    Returns:
        None

    Exceptions:
        ValueError if the variable does not exist in the input file
    """

    logger.info("Creating variable %s", variable_name)

    # Copy variable metadata
    variable = input_file.variables[variable_name]

    if variable_name in [time_variable, reference_time_variable]:
        # Time variables
        new_variable_dimensions = variable.dimensions
    else:
        new_variable_dimensions = variable.dimensions + (time_variable,)

    # Set chunking information
    chunk_size = [chunk_rows] * (len(new_variable_dimensions) - 1)
    chunk_size.append(len(input_filepaths))

    output_variable = output_file.createVariable(
        variable_name,
        variable.dtype,
        new_variable_dimensions,
        chunksizes=chunk_size,
        zlib=True,
        complevel=4,
        shuffle=True,
    )

    # Copy variable attributes
    copy_attributes(variable, output_variable)

    # Load variable arrays
    variable_arrays = []

    # Calculate processing chunk list
    processing_chunks = create_processing_chunks(
        len(input_filepaths),
        len(input_file.dimensions[variable.dimensions[0]]),
        chunk_rows,
        max_memory,
    )

    # Time data may have to be adjusted as the reference date and
    # units might not match the primary input file (archetype).
    # Fortunately, the date2num and num2date functions take care
    # of this.
    if variable_name in [time_variable, reference_time_variable]:

        for input_data_file in input_filepaths:
            with Dataset(input_data_file, "r") as data_frame:

                # Note that date2num outputs double types, so there may be
                # rounding issues
                variable_arrays.append(
                    date2num(
                        num2date(
                            data_frame.variables[variable_name][:],
                            data_frame.variables[variable_name].units,
                        ),
                        input_file.variables[variable_name].units,
                    )
                )

        logger.info("Restacking variable %s", variable_name)

        if str(output_variable.dtype).startswith("int"):
            # Have to make sure any double type date output is correctly
            # rounded to any integer type. The default behavior is to truncate
            # any precision.
            output_variable[:] = numpy.rint(numpy.column_stack(variable_arrays))
        else:
            output_variable[:] = numpy.column_stack(variable_arrays)

        # Flush contents
        output_file.sync()

    else:

        # Loop through chunks in order to minimize memory footprints
        logger.info(
            "Restacking %s using %d processing chunks",
            variable_name,
            len(processing_chunks),
        )

        for chunk_range in processing_chunks:

            variable_arrays = []

            for input_data_file in input_filepaths:
                # This repeated reopening/reclosing of the files is not
                # optimal, performance-wise. Could be threaded?
                with Dataset(input_data_file, "r") as data_frame:

                    # Double check variable existence
                    if variable_name not in data_frame.variables:
                        raise ValueError(
                            "Variable {0} not present in "
                            "{1}".format(variable_name, input_data_file)
                        )

                    # Regular variable, no conversion needed
                    # At the moment, input variable is assumed to be one dimensional
                    variable_arrays.append(
                        data_frame.variables[variable_name][
                            chunk_range[0] : chunk_range[1]
                        ]
                    )

            output_variable[chunk_range[0] : chunk_range[1], :] = numpy.column_stack(
                variable_arrays
            )

            # Flush contents
            output_file.sync()

    logger.info("Done restacking variable %s", variable_name)


############################################################################
#
#  Create time series
#
############################################################################
def create_time_series(
    input_filepaths,
    output_filepath,
    data_variables,
    time_variable,
    reference_time_variable,
    chunk_length,
    max_memory,
    removed_global_attributes,
    skip_missing,
):
    """Create a time series out of NetCDF files

    This concatenates a list of NetCDF files into one time series.

    Args:
        input_filepaths (list of strings): A list of input file paths to
             concatenates. The first file will be used a as a template to
             create the output file.
        output_filepath (string): The filepath of the output file that will
             be created.
        data_variables (list of strings): A list containing the variable(s)
             to concatenate into a time series.
        time_variable (string): The name of the time variable to use
        reference_time_variables (string): The name of the reference time
             variable to use.
        chunk_length (integer): The number of rows that a chunk will span
        max_memory (integer): The approximate maximum number of bytes of
            memory that the function should use.
        removed_global_attributes (list of strings): A list of global
            attribute names that will not be copied to the output file
        skip_missing (Boolean): Whether or not to skip input files that
            are missing variables

    Returns:
        None

    Exceptions:
        None
    """

    # Cleanup flags
    remove_output = False

    try:
        # Validate input files
        input_filepaths = validate_variables(
            input_filepaths,
            data_variables + [time_variable, reference_time_variable],
            skip_missing,
        )
        input_filepaths = validate_common_dimensions(input_filepaths, skip_missing)

        # Open output file
        with Dataset(
            output_filepath, "w", clobber=True, format="NETCDF4"
        ) as output_file:
            remove_output = True
            logger.info("Creating %s", output_filepath)

            # Copy base information from the first entry (archetype)
            with Dataset(input_filepaths[0], "r") as archetype:

                # Copy dimensions from the archetype
                copy_dimensions(
                    archetype,
                    output_file,
                    [time_variable, reference_time_variable],
                    len(input_filepaths),
                )

                # Copy coordinate variables from the archetype
                # (excluding time-related ones)
                copy_coordinate_variables(
                    archetype, output_file, [time_variable, reference_time_variable]
                )

                # Copy global attributes from the archetype
                copy_attributes(archetype, output_file, removed_global_attributes)

                # Also add time and reference time variables, if specified
                if time_variable:
                    data_variables.append(time_variable)
                if reference_time_variable:
                    data_variables.append(reference_time_variable)

                # Loop through specified variables
                for variable in data_variables:

                    # Create and fill variable
                    create_time_series_variable(
                        archetype,
                        output_file,
                        variable,
                        time_variable,
                        reference_time_variable,
                        input_filepaths,
                        chunk_length,
                        max_memory,
                    )

        # Success
        remove_output = False

    except BaseException:

        # Cleanup
        if remove_output:
            os.remove(output_filepath)

        raise


############################################################################
#
#  "Main"
#
############################################################################
def main():
    """Main

    This program attempts to concatenate several NetCDF files containing one
    dimensional variables for a given time into file containing a two
    dimensional time series.

    Args:
        None (see setup_arguments for command line arguments)

    Returns:
        None

    Exceptions:
        None
    """

    try:
        # Set up logging
        init_logging()

        # Get command line arguments
        # This can raise SystemExit if the help option is requested
        options = setup_arguments()

        # Further configure logging
        set_logging_options(options)

        # Do work
        create_time_series(
            options.input_files,
            options.output_file,
            options.data_variables,
            options.time_variable,
            options.reference_time_variable,
            options.chunk_size,
            options.max_memory,
            options.removed_global_attributes,
            options.skip_missing,
        )

        # Terminate logging interface. This might be automatic
        logging.shutdown()

        # Success
        return

    except SystemExit:
        return
    except BaseException:

        # Log any exception message
        logger.error(format_current_exception())

        # Terminate logging interface. This might be automatic
        logging.shutdown()

        # Failure
        sys.exit(1)


# Is this main?
if __name__ == "__main__":
    main()
