# Canonical name map: raw token → display name
# Lets us unify "py", "python3", "python 3" → "Python"
from __future__ import annotations

_CANONICAL: dict[str, str] = {
    # Languages
    "python":       "Python",
    "python3":      "Python",
    "python 3":     "Python",
    "py":           "Python",
    "java":         "Java",
    "java ee":      "Java EE",
    "kotlin":       "Kotlin",
    "scala":        "Scala",
    "javascript":   "JavaScript",
    "js":           "JavaScript",
    "typescript":   "TypeScript",
    "ts":           "TypeScript",
    "php":          "PHP",
    "ruby":         "Ruby",
    "go":           "Go",
    "golang":       "Go",
    "rust":         "Rust",
    "c#":           "C#",
    "csharp":       "C#",
    "c++":          "C++",
    "cpp":          "C++",
    "c":            "C",
    "swift":        "Swift",
    "r":            "R",
    "matlab":       "MATLAB",
    "perl":         "Perl",
    "bash":         "Bash",
    "shell":        "Shell",
    "powershell":   "PowerShell",

    # Web / frontend
    "html":         "HTML",
    "html5":        "HTML",
    "css":          "CSS",
    "css3":         "CSS",
    "sass":         "Sass",
    "react":        "React",
    "reactjs":      "React",
    "react.js":     "React",
    "vue":          "Vue.js",
    "vuejs":        "Vue.js",
    "vue.js":       "Vue.js",
    "angular":      "Angular",
    "angularjs":    "AngularJS",
    "svelte":       "Svelte",
    "next.js":      "Next.js",
    "nextjs":       "Next.js",
    "nuxt":         "Nuxt.js",
    "gatsby":       "Gatsby",

    # Backend / frameworks
    "django":       "Django",
    "flask":        "Flask",
    "fastapi":      "FastAPI",
    "spring":       "Spring",
    "spring boot":  "Spring Boot",
    "laravel":      "Laravel",
    "symfony":      "Symfony",
    "express":      "Express.js",
    "express.js":   "Express.js",
    "node":         "Node.js",
    "node.js":      "Node.js",
    "nodejs":       "Node.js",
    "rails":        "Ruby on Rails",
    "ruby on rails":"Ruby on Rails",
    "asp.net":      "ASP.NET",
    "dotnet":       ".NET",
    ".net":         ".NET",

    # Data / ML
    "pandas":       "pandas",
    "numpy":        "NumPy",
    "scipy":        "SciPy",
    "scikit-learn": "scikit-learn",
    "sklearn":      "scikit-learn",
    "tensorflow":   "TensorFlow",
    "pytorch":      "PyTorch",
    "keras":        "Keras",
    "spark":        "Apache Spark",
    "apache spark": "Apache Spark",
    "pyspark":      "PySpark",
    "kafka":        "Apache Kafka",
    "apache kafka": "Apache Kafka",
    "hadoop":       "Hadoop",
    "airflow":      "Apache Airflow",
    "dbt":          "dbt",
    "looker":       "Looker",
    "tableau":      "Tableau",
    "power bi":     "Power BI",
    "powerbi":      "Power BI",
    "elasticsearch":"Elasticsearch",
    "elastic":      "Elasticsearch",
    "kibana":       "Kibana",
    "logstash":     "Logstash",

    # Cloud
    "aws":          "AWS",
    "amazon web services": "AWS",
    "azure":        "Azure",
    "microsoft azure":     "Azure",
    "gcp":          "GCP",
    "google cloud": "GCP",
    "s3":           "AWS S3",
    "ec2":          "AWS EC2",
    "lambda":       "AWS Lambda",
    "cloudformation": "CloudFormation",
    "terraform":    "Terraform",
    "pulumi":       "Pulumi",

    # Containers / DevOps
    "docker":       "Docker",
    "kubernetes":   "Kubernetes",
    "k8s":          "Kubernetes",
    "helm":         "Helm",
    "jenkins":      "Jenkins",
    "gitlab":       "GitLab",
    "gitlab ci":    "GitLab CI",
    "github":       "GitHub",
    "github actions": "GitHub Actions",
    "circleci":     "CircleCI",
    "ansible":      "Ansible",
    "puppet":       "Puppet",
    "chef":         "Chef",
    "prometheus":   "Prometheus",
    "grafana":      "Grafana",
    "nginx":        "Nginx",
    "apache":       "Apache HTTP",

    # Databases
    "sql":          "SQL",
    "mysql":        "MySQL",
    "postgresql":   "PostgreSQL",
    "postgres":     "PostgreSQL",
    "sqlite":       "SQLite",
    "oracle":       "Oracle DB",
    "mssql":        "SQL Server",
    "sql server":   "SQL Server",
    "mongodb":      "MongoDB",
    "mongo":        "MongoDB",
    "redis":        "Redis",
    "cassandra":    "Cassandra",
    "neo4j":        "Neo4j",
    "dynamodb":     "DynamoDB",
    "bigquery":     "BigQuery",
    "snowflake":    "Snowflake",
    "redshift":     "Redshift",

    # Protocols / APIs
    "rest":         "REST",
    "restful":      "REST",
    "graphql":      "GraphQL",
    "grpc":         "gRPC",
    "soap":         "SOAP",
    "openapi":      "OpenAPI",
    "swagger":      "Swagger",
    "json":         "JSON",
    "xml":          "XML",
    "websocket":    "WebSocket",

    # Methodologies
    "agile":        "Agile",
    "scrum":        "Scrum",
    "kanban":       "Kanban",
    "devops":       "DevOps",
    "ci/cd":        "CI/CD",
    "cicd":         "CI/CD",
    "tdd":          "TDD",
    "bdd":          "BDD",
    "mlops":        "MLOps",
    "dataops":      "DataOps",
    "gitflow":      "Git Flow",
    "git":          "Git",

    # Security
    "oauth":        "OAuth",
    "oauth2":       "OAuth2",
    "jwt":          "JWT",
    "ssl":          "SSL/TLS",
    "tls":          "SSL/TLS",
    "ldap":         "LDAP",
    "saml":         "SAML",
    "sso":          "SSO",

    # Testing
    "junit":        "JUnit",
    "pytest":       "pytest",
    "jest":         "Jest",
    "cypress":      "Cypress",
    "selenium":     "Selenium",
    "sonarqube":    "SonarQube",
}


# ──────────────────────────────────────────────
# Regex patterns for free-text scan
# ──────────────────────────────────────────────

# Multi-word patterns first (order matters — longest match wins)
_MULTIWORD_PATTERNS: list[str] = [
    r"spring\s+boot",
    r"ruby\s+on\s+rails",
    r"apache\s+spark",
    r"apache\s+kafka",
    r"apache\s+airflow",
    r"amazon\s+web\s+services",
    r"microsoft\s+azure",
    r"google\s+cloud",
    r"machine\s+learning",
    r"deep\s+learning",
    r"natural\s+language\s+processing",
    r"large\s+language\s+model",
    r"power\s+bi",
    r"github\s+actions",
    r"gitlab\s+ci(?:/cd)?",
    r"ci/cd",
    r"node\.js",
    r"next\.js",
    r"nuxt\.js",
    r"express\.js",
    r"vue\.js",
    r"asp\.net",
    r"scikit[\-\s]learn",
    r"sql\s+server",
    r"java\s+ee",
    r"c\+\+",
    r"c#",
]

# Single-word / short tokens (word-boundary anchored)
_SINGLEWORD_PATTERNS: list[str] = [
    r"python3?", r"java(?!script)", r"kotlin", r"scala",
    r"javascript", r"typescript", r"php", r"ruby", r"golang?",
    r"rust", r"swift", r"matlab", r"perl", r"bash", r"powershell",
    r"html5?", r"css3?", r"sass", r"react", r"angular", r"svelte",
    r"gatsby", r"django", r"flask", r"fastapi", r"laravel", r"symfony",
    r"rails", r"dotnet", r"pandas", r"numpy", r"scipy", r"tensorflow",
    r"pytorch", r"keras", r"pyspark", r"hadoop", r"airflow", r"dbt",
    r"looker", r"tableau", r"elasticsearch", r"kibana", r"logstash",
    r"aws", r"azure", r"gcp", r"s3", r"ec2", r"lambda", r"terraform",
    r"gcp", r"bigquery", r"snowflake", r"redshift",
    r"azure", r"azure synapse", r"azure data factory", r"azure data warehouse", r"azure data lake",
    r"pulumi", r"docker", r"kubernetes", r"k8s", r"helm", r"jenkins",
    r"gitlab", r"github", r"circleci", r"ansible", r"puppet",
    r"prometheus", r"grafana", r"nginx", r"mysql", r"postgresql",
    r"postgres", r"sqlite", r"mongodb", r"mongo", r"redis",
    r"cassandra", r"neo4j", r"dynamodb", r"bigquery", r"snowflake",
    r"redshift", r"graphql", r"grpc", r"soap", r"openapi", r"swagger",
    r"websocket", r"agile", r"scrum", r"kanban", r"devops", r"cicd",
    r"tdd", r"bdd", r"mlops", r"dataops", r"git",
    r"oauth2?", r"jwt", r"ldap", r"saml", r"sso",
    r"junit", r"pytest", r"jest", r"cypress", r"selenium", r"sonarqube",r"spark",r"kafka",r"spring",r"powerBI",r"dbt",r"looker",r"tableau",
    r"pytorch",r"keras",r"pandas",r"numpy",r"scipy",r"matplotlib",r"seaborn",r"plotly"
]


# ──────────────────────────────────────────────
# Skill rollup taxonomy
# ──────────────────────────────────────────────
# child canonical name → parent canonical name
# When a child is found its weight is added to the parent and the child
# is removed from the output entirely.

ROLLUP_MAP: dict[str, str] = {
    # AWS sub-services → AWS
    "AWS S3":           "AWS",
    "AWS EC2":          "AWS",
    "AWS Lambda":       "AWS",
    "CloudFormation":   "AWS",
    "DynamoDB":         "AWS",
    "Redshift":         "AWS",

    # Azure sub-services → Azure
    "Azure DevOps":     "Azure",

    # GCP sub-services → GCP
    "BigQuery":         "GCP",

    # Git ecosystem → Git
    "GitLab":           "Git",
    "GitLab CI":        "Git",
    "GitHub":           "Git",
    "GitHub Actions":   "Git",
    "Git Flow":         "Git",

    # CI/CD tools → CI/CD
    "Jenkins":          "CI/CD",
    "CircleCI":         "CI/CD",

    # Kubernetes tooling → Kubernetes
    "Helm":             "Kubernetes",

    # ELK stack → Elasticsearch
    "Kibana":           "Elasticsearch",
    "Logstash":         "Elasticsearch",

    # Apache umbrella
    "PySpark":          "Apache Spark",

    # Spring family → Spring
    "Spring Boot":      "Spring",

    # SQL dialects → SQL  (optional — comment out if you want granularity)
    # "MySQL":          "SQL",
    # "PostgreSQL":     "SQL",
    # "SQLite":         "SQL",
    # "SQL Server":     "SQL",
    # "Oracle DB":      "SQL",

    # Agile methodologies → Agile
    "Scrum":            "Agile",
    "Kanban":           "Agile",

    # Security tokens → OAuth
    "OAuth2":           "OAuth",
}