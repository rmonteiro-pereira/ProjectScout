import os
import argparse
import json

DEFAULT_CONFIG = {
    "project_name": "My Project",
    "description": "",
    "author_name": "Rodrigo Monteiro Pereira",
    "layers": [
        "raw",
        "intermediate",
        "primary",
        "feature"
    ]
}

def create_project_structure(base_path, config):
    project_name = config["project_name"]
    layers = config.get("layers", [])

    # Directory structure
    project_structure = [
        "conf/base",
        "conf/local",
        "notebooks",
        "dags",
        f"src/{project_name}"
    ]

    # Add dynamic data and pipeline layers from config
    for idx, layer in enumerate(layers, start=1):
        project_structure.append(f"data/{str(idx).zfill(2)}_{layer}")
        project_structure.append(f"src/{project_name}/pipelines/{layer}")

    # Files to create
    files_to_create = {
        "README.md": f"# {project_name.replace('_', ' ').title()}\n\n{config['description']}\n",
        ".gitignore": "*.pyc\n__pycache__/\ndata/\nconf/local/\n.env",
        "LICENSE": "MIT License",
        f"src/{project_name}/__init__.py": "",
        f"src/{project_name}/hooks.py": "# Kedro project hooks",
        f"src/{project_name}/settings.py": "# Kedro project settings",
        f"dags/{project_name}_pipeline_dag.py": f"# Airflow DAG for the {project_name.replace('_', ' ').title()} pipeline",
        "pyproject.toml": f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "{config['description']}"
authors = [
  {{ name = "{config['author_name']}" }}
]
dependencies = [
  "kedro",
  "pyspark",
  "apache-airflow",
  "great-expectations",
  "pandas",
  "requests"
]
requires-python = ">=3.10"
"""
    }

    # Dynamically create node and pipeline files per layer
    for layer in layers:
        files_to_create[f"src/{project_name}/pipelines/{layer}/__init__.py"] = ""
        files_to_create[f"src/{project_name}/pipelines/{layer}/nodes.py"] = f"# Nodes for the {layer} layer"
        files_to_create[f"src/{project_name}/pipelines/{layer}/pipeline.py"] = f"# Pipeline definition for the {layer} layer"

    # Create folders
    for folder in project_structure:
        full_path = os.path.join(base_path, folder)
        os.makedirs(full_path, exist_ok=True)
        print(f"📂 Created directory: {full_path}")

    # Create files
    for file_path, content in files_to_create.items():
        full_file_path = os.path.join(base_path, file_path)
        dir_name = os.path.dirname(full_file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        with open(full_file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"📄 Created file: {full_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Open Finance Lakehouse project setup.")
    parser.add_argument("--config_file", type=str, help="Path to the JSON configuration file.")
    parser.add_argument("--path", type=str, default=".", help="Path where the project should be created. Default is current directory.")
    args = parser.parse_args()

    # Load configuration
    if args.config_file:
        with open(args.config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
    else:
        print("⚠️  Nenhum arquivo de configuração informado.")
        project_name = input(f"Nome do projeto [{DEFAULT_CONFIG['project_name']}]: ").strip() or DEFAULT_CONFIG['project_name']
        description = input(f"Descrição [{DEFAULT_CONFIG['description']}]: ").strip() or DEFAULT_CONFIG['description']
        config = DEFAULT_CONFIG.copy()
        config["project_name"] = project_name
        config["description"] = description

    project_path = os.path.abspath(args.path)
    os.makedirs(project_path, exist_ok=True)
    print(f"🚀 Setting up project at: {project_path}")

    create_project_structure(base_path=project_path, config=config)
