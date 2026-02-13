# Oracle CLI (Oracle Prompt)

CLI interactif pour exécuter des requêtes Oracle, lancer des scripts SQL et insérer des fichiers CSV en bulk dans une table Oracle, avec exécution asynchrone via un `ThreadPoolExecutor` et logging par tâche.

## Fonctionnalités

- **REPL** (prompt) : exécution de requêtes SQL interactives
- **Exécution asynchrone** des commandes (sauf `querysync`) avec un identifiant de tâche
- **Logs & outputs** par tâche dans un répertoire dédié
- **Exécution de scripts SQL** (`runscript`) avec découpage en requêtes via `sqlparse`
- **Export CSV** des résultats de requêtes (`query` / `queryc` / `querysync`)
- **Insertion bulk depuis CSV** (`insertmany`) avec batching (50000 lignes)

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