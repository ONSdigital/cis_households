import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def read_requirements(file):
    with open(file) as f:
        return f.read().splitlines()


requires = read_requirements("requirements.txt")


dev_requires = ["pre-commit==2.12.1", "detect-secrets==1.0.3"] + requires

setuptools.setup(
    name="cishouseholds",
    version="0.0.1",
    author="cis dev team",
    author_email="cis.dev@ons.gov.uk",
    description=(
        "Data engineering pipeline for the Office for National"
        " Statistics COVID-19 Infection Survey (CIS)"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ONS-SST/cis_households",
    project_urls={
        "Bug Tracker": "https://github.com/ONS-SST/cis_households/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(where="src"),
    python_requires="==3.6.8",
    install_requires=requires,
    extras_require={"dev": dev_requires},
)
