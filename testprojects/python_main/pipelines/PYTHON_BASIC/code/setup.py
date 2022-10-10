from setuptools import setup, find_packages
setup(
    name = 'PYTHON_BASIC',
    version = '1.0',
    packages = find_packages(include = ('pythonbasic.test.mainone*', )),
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.4'],
    entry_points = {
'console_scripts' : [
'main = pythonbasic.test.mainone.pipeline:main', ], },
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
