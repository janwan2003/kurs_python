from setuptools import find_packages, setup

setup(
    name="bus_analysis",
    packages=find_packages(exclude=["bus_analysis_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={
        "dev": [
            "dagster-webserver",
            "pytest",
            "pandas",
            "geopy",
            "scikit-learn",
            "folium",
            "pylint",
        ]
    },
)
