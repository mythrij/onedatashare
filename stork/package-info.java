/**
 * Root package for Stork, containing classes for launching the application and
 * providing access to global configuration. Most command line commands will
 * delegate execution to classes in the {@code stork.client} package. One
 * exception is the {@code server} command, which is implemented in {@code
 * StorkServer} and for the most part delegates to classes in the {@code
 * stork.scheduler} package.
 *
 * Other functionality is organized into other subpackages. More information
 * can be found in their respective documentation.
 */
package stork;
