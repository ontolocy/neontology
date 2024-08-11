{
  description = "Easily ingest data into a Neo4j graph database with Python, pandas and Pydantic";

  inputs = {
    #nixpkgs.url = "github:nixos/nixpkgs/24.05";
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs@{ flake-parts, nixpkgs, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [];
      systems = [ "x86_64-linux" "aarch64-linux" ];
      perSystem = { config, self', inputs', pkgs, system, ... }:
      let
        project = pkgs.callPackage ./package.nix {
          buildPythonPackage = nixpkgs.legacyPackages.${system}.python3.pkgs.buildPythonPackage;
          python = nixpkgs.legacyPackages.${system}.python3;
        };
        my_python = pkgs.python3.withPackages (ps: [
          project
        ]);
      in { 
        packages.default = project;
        devShells.default = pkgs.mkShell {
          packages = [ my_python ];
        };
      };
      flake = {
      };
    };
}
