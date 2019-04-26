from setuptools import setup, find_packages

setup(
    name = "pk1-remedy",
    version = "0.0.5",
    keywords = ("pip", "packone"),
    description = "Scripts to remedy vm images (of packone).",
    long_description = open('README.rst').read(),
    license = "Apache-2.0 Licence",
    url = "https://github.com/cas-bigdatalab/packone_remedy",
    author = "Excel Wang",
    author_email = "wanghj@cnic.com",
    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ["requests"],
    entry_points = {
        'console_scripts': [
            'ambari-host-clone = pk1.remedy.ambari.host_clone:main',
            'ambari-host-delete = pk1.remedy.ambari.host_delete:main',
            'ambari-service-start = pk1.remedy.ambari.service_start:main'
        ]
    }
)