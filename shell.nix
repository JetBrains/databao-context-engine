{ pkgs ? import <nixpkgs> { }
,
}:
let
  projectDir = builtins.toString ./.;
in
pkgs.mkShell {
  shellHook = ''
    alias dce='uv --project ${projectDir} run dce'
  '';
}
