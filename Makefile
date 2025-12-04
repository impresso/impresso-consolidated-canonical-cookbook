# Description: Makefile for consolidated canonical processing
# Read the README.md for more information on how to use this Makefile.
# Or run `make` for online help.

#### ENABLE LOGGING FIRST
# USER-VARIABLE: LOGGING_LEVEL
# Defines the logging level for the Makefile.

# Load make logging function library
include cookbook/log.mk


# USER-VARIABLE: CONFIG_LOCAL_MAKE
# Defines the name of the local configuration file to include.
#
# This file is used to override default settings and provide local configuration. If a
# file with this name exists in the current directory, it will be included. If the file
# does not exist, it will be silently ignored. Never add the file called config.local.mk
# to the repository! If you have stored config files in the repository set the
# CONFIG_LOCAL_MAKE variable to a different name.
CONFIG_LOCAL_MAKE ?= config.local.mk
ifdef CFG
  CONFIG_LOCAL_MAKE := $(CFG)
  $(info Overriding CONFIG_LOCAL_MAKE to $(CONFIG_LOCAL_MAKE) from CFG variable)
else
  $(call log.info, CONFIG_LOCAL_MAKE)
endif
# Load local config if it exists (ignore silently if it does not exists)
-include $(CONFIG_LOCAL_MAKE)


# Report logging level after processing local configurations
  $(call log.info, LOGGING_LEVEL)


#: Show help message
help::
	@echo "Makefile for consolidated canonical processing"
	@echo "Usage: make <target> PROVIDER=<provider> NEWSPAPER=<newspaper>"
	@echo "Targets:"
	@echo "  setup                 # Prepare the local directories"
	@echo "  collection            # Process multiple newspapers in parallel"
	@echo "  all                   # Sync data and process all years of a single newspaper"
	@echo "  newspaper             # Process a single newspaper for all years"
	@echo "  sync                  # Sync input data (canonical + langident enrichments)"
	@echo "  sync-input            # Sync only input data"
	@echo "  sync-output           # Sync output data from S3"
	@echo "  resync                # Remove local sync stamps and sync again"
	@echo "  clean-build           # Remove the entire build directory"
	@echo "  clean-newspaper       # Remove local directory for a single newspaper"
	@echo "  help                  # Show this help message"
	@echo "  help-orchestration    # Show detailed orchestration and parallelization help"

# Default target when no target is specified on the command line
.DEFAULT_GOAL := help
.PHONY: help


# Set shared make options
include cookbook/make_settings.mk

# If you need to use a different shell than /bin/dash, overwrite it here.
# SHELL := /bin/bash



# SETUP SETTINGS AND TARGETS
include cookbook/setup.mk
include cookbook/setup_python.mk
# for asw tool configuration if needed
# include cookbook/setup_aws.mk
# for consolidatedcanonical configuration
include cookbook/setup_CONSOLIDATEDCANONICAL.mk

# Load newspaper list configuration and processing rules
include cookbook/newspaper_list.mk


# SETUP PATHS
# include all path makefile snippets for s3 collection directories that you need
include cookbook/paths_canonical.mk
include cookbook/paths_langident.mk
include cookbook/paths_CONSOLIDATEDCANONICAL.mk


# MAIN TARGETS
include cookbook/main_targets.mk


# SYNCHRONIZATION TARGETS
include cookbook/sync.mk
include cookbook/sync_canonical.mk
include cookbook/sync_langident.mk
include cookbook/sync_CONSOLIDATEDCANONICAL.mk

include cookbook/clean.mk


# PROCESSING TARGETS
include cookbook/processing.mk
include cookbook/processing_CONSOLIDATEDCANONICAL.mk


# FUNCTION
include cookbook/local_to_s3.mk


# FURTHER ADDONS
# configure for aws client access
include cookbook/aws.mk

# Add help target with configuration documentation
help::
	@echo ""
	@echo "CONFIGURATION:"
	@echo "  Use CFG=<file> to specify a custom configuration file"
	@echo "  Example: make newspaper CFG=config.prod.mk PROVIDER=BL NEWSPAPER=WTCH"
	@echo ""
	@echo "REQUIRED VARIABLES:"
	@echo "  PROVIDER          #  Data provider organization (e.g., BL, SWA, NZZ)"
	@echo "  NEWSPAPER         #  Target newspaper to process (e.g., WTCH, actionfem)"
	@echo ""
	@echo "For detailed information about processing configuration, parallelization,"
	@echo "performance tuning, and examples, run: make help-orchestration"
	@echo ""
