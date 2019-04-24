from setuptools import setup, find_packages

setup(
    name = "packone-remedy",
    version = "0.0.1",
    keywords = ("pip", "packone"),
    description = "Scripts to remedy vm images (of packone)",
    long_description = "Scripts to remedy vm images (of packone)",
    license = "Apache-2.0 Licence",
    url = "https://github.com/cas-bigdatalab/packone_remedy",
    author = "Excel Wang",
    author_email = "wanghj@cnic.com",
    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ["requests"]
)