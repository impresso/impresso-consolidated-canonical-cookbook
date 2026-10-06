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
	@echo ""
	@echo "Usage: make <target> PROVIDER=<provider> NEWSPAPER=<newspaper>"
	@echo ""
	@echo "MAIN TARGETS:"
	@echo "  make newspaper            # Sync and process a single newspaper for all years"
	@echo "  make collection           # Process multiple newspapers in parallel"
	@echo "  make all                  # Force input/output resync, then process a single newspaper"
	@echo "  make setup                # Prepare the local directories"
	@echo "  make sync                 # Sync input (canonical + langident enrichments) and output data"
	@echo "  make sync-input           # Sync only input data"
	@echo "  make sync-output          # Sync only output data from S3"
	@echo "  make resync               # Remove local sync stamps and sync again"
	@echo "  make clean-build          # Remove the entire build directory"
	@echo ""
	@echo "REQUIRED VARIABLES:"
	@printf '  %-24s %s\n' 'PROVIDER=$(PROVIDER)' 'Data provider organization (e.g., BL, SWA, NZZ)'
	@printf '  %-24s %s\n' 'NEWSPAPER=$(NEWSPAPER)' 'Newspaper to process (e.g., WTCH, actionfem)'
	@echo ""
	@echo "CONFIGURATION:"
	@echo "  make newspaper CFG=config.prod.mk PROVIDER=BL NEWSPAPER=WTCH  # Use a custom configuration file"
	@echo ""
	@echo "MORE HELP:"
	@echo "  make help-orchestration   # Collection runs, parallelization, tuning, S3 deletion preview"
	@echo "  make help-sync            # Sync targets and S3 refresh behavior"
	@echo "  make help-processing      # Processing entry point and flags"
	@echo "  make help-setup           # Python environment setup"
	@echo "  make help-clean           # Clean targets"
	@echo "  make help-aws             # AWS CLI setup and S3 folder moves"
	@echo "  make help-newspaper-list  # Collection list generation"
	@echo "  make help-path-variables  # Resolved S3 and local paths"
	@echo ""

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
include cookbook/setup_consolidatedcanonical.mk

# Load newspaper list configuration and processing rules
include cookbook/newspaper_list.mk


# SETUP PATHS
# include all path makefile snippets for s3 collection directories that you need
include cookbook/paths_canonical.mk
include cookbook/paths_langident.mk
include cookbook/paths_consolidatedcanonical.mk


# MAIN TARGETS
include cookbook/main_targets.mk


# SYNCHRONIZATION TARGETS
include cookbook/sync.mk
include cookbook/sync_canonical.mk
include cookbook/sync_langident.mk
include cookbook/sync_consolidatedcanonical.mk

include cookbook/clean.mk


# PROCESSING TARGETS
include cookbook/processing.mk
include cookbook/processing_consolidatedcanonical.mk

# Explicit task selection belongs to this pipeline, not shared path fragments.
COMPLETENESS_TASK ?= consolidatedcanonical
  $(call log.info, COMPLETENESS_TASK)
include cookbook/completeness.mk


# FUNCTION
include cookbook/local_to_s3.mk


# FURTHER ADDONS
# configure for aws client access
include cookbook/aws.mk

