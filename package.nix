{ lib
, pkgs
, buildPythonPackage
, fetchFromGitHub
, python
}:

buildPythonPackage rec {
  pname = "neotology";
  version = "dev";

  src = ./.;

  build-system = [ python.pkgs.setuptools ];
  pyproject = true;

  dependencies = with python.pkgs; [
    neo4j
    pydantic
    pandas
    numpy
    python-dotenv
  ];

  pythonImportsCheck = [
    "neontology"
  ];
}
