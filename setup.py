from setuptools import setup, find_packages

setup(
    name = "ambari",
    version = "0.1.7",
    keywords = ("pip", "ambari", "packone"),
    description = "Amabri python client based on ambari rest api.",
    long_description = open('README.rst').read(),
    license = "Apache-2.0 Licence",
    url = "https://github.com/excelwang/ambari-python-client",
    author = "Excel Wang",
    author_email = "wanghj@cnic.com",
    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ["requests"],
    entry_points = {
        'console_scripts': [
            'ambari = ambari.cmd:run',
        ]
    }
)