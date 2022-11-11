from setuptools import setup, find_packages
setup(
    name = 'PythonDoAnything',
    version = '1.0',
    packages = find_packages(include = ['pythondoanything*', ]),
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.3'],
    entry_points = {
'console_scripts' : [
'main = pythondoanything.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
