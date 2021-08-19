from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='labelspark',
      version='0.4.0',
      packages=find_packages(),
      url='https://github.com/Labelbox/LabelSpark.git',
      description='Labelbox & Databricks integration helper library',
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=["labelbox"],
      extras_require={'dev': ['pylint']})
