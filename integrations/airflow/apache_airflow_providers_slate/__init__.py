def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-slate",
        "name": "Slate",
        "description": "High-throughput multi-cloud data transfers for AI workloads.",
        "versions": ["0.2.0"],
        "connection-types": [
            {
                "connection-type": "slate",
                "hook-class-name": "apache_airflow_providers_slate.hooks.slate.SlateHook",
            }
        ],
    }
