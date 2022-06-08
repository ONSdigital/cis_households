import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def read_requirements(file):
    with open(file) as f:
        return f.read().splitlines()


requires = read_requirements("requirements.txt")


dev_requires = [
    "pre-commit==2.12.1",
    "detect-secrets==1.0.3",
    "pytest>=3.6,<4",
    "bump2version==1.0.1",
    "chispa==0.8.2",
    "pytest-regressions==2.2.0",
    "pandas>=1.0.0",
    "sphinx==4.4.0",
    "pydata-sphinx-theme==0.7.2",
] + requires

setuptools.setup(
    name="cishouseholds",
    version="1.3.3-beta.11",
    author="CIS development team",
    author_email="cis.dev@ons.gov.uk",
    description="Data engineering pipeline for the Office for National Statistics COVID-19 Infection Survey (CIS)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ONS-SST/cis_households",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(exclude="tests"),
    python_requires="==3.6.8",
    install_requires=requires,
    extras_require={"dev": dev_requires, "ci": dev_requires + ["pyspark==2.4.1", "pytest-cov", "coverage"]},
)
