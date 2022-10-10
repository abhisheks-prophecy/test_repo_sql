from setuptools import setup, find_packages
setup(
    name = 'PYTHON_GCP',
    version = '1.0',
    packages = find_packages(include = ('pythondoanything*', )),
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.4'],
    entry_points = {
'console_scripts' : [
'main = pythondoanything.pipeline:main', ], },
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
