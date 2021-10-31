import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pypgoutput",
    version="0.0.1",
    author="Daniel Geals",
    author_email="danielgeals@gmail.com",
    description="Read py-pgoutput messages",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dgea005/py-pgoutput",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
