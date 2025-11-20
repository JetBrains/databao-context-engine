{ pkgs ? import <nixpkgs> { }
,
}:
let
  projectDir = builtins.toString ./.;
in
pkgs.mkShell {
  shellHook = ''
    alias nemory='uv --project ${projectDir} run nemory'
  '';
}
