import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="decase",
    version="0.0.1",
    author="Herculano Cunha",
    author_email="herculanocm@outlook.com",
    description="Data Engineer tips for Airflow",
    download_url='https://github.com/herculanocm/de_case/archive/master.zip',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    keywords='tips python py3.11',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.11',
    install_requires=['pyspark', 'boto3', 'build']
)