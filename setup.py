import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="logprov",
    version="0.0.2",
    author="Mathieu Servillat & Jose Enrique Ruiz",
    author_email="mathieu.servillat@obspm.fr, jer@iaa.es",
    description="capture of provenance information into structured logs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mservillat/logprov",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)