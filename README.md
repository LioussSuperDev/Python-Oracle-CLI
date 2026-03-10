# Oracle CLI (Oracle Prompt)

CLI interactif pour exécuter des requêtes Oracle, lancer des scripts SQL et insérer des fichiers CSV en bulk dans une table Oracle, avec exécution asynchrone via un `ThreadPoolExecutor` et logging par tâche.

## Structure attendue

Le projet attend les fichiers suivants à côté du script principal :

- `ORACLE_IDENTIFIER.json` : identifiants de connexion Oracle
- `config.json` : configuration (au minimum le dossier d’output)

## Installation

```bash
pip install "liouss-python-oracle-cli @ git+https://github.com/LioussSuperDev/Python-Oracle-CLI.git@stable"
```

OU

```bash
git clone https://github.com/LioussSuperDev/Python-Oracle-CLI.git
cd Python-Oracle-CLI
pip install -e .
```

## Lancement

```bash
oracle
```

Sur Linux : pensez à ajouter au path "~/.local/bin/", ou executez :

```bash
~/.local/bin/oracle
```