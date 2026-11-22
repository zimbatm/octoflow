# The landing page, apart from the binary: a change to the page must not
# change the store path of the services, or a deploy restarts them all.
{ pkgs, ... }:
pkgs.runCommandLocal "octoflow-web" {
  src = pkgs.lib.fileset.toSource {
    root = ../../web;
    fileset = ../../web;
  };
} "cp -r $src $out"
