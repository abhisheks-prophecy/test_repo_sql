from setuptools import setup, find_packages
setup(
    name = 'DONOT_OPEN_PYTHON',
    version = '1.0',
    packages = find_packages(include = ['job*', ]),
    description = 'workflow',
    install_requires = ['matplotlib==3.5.2', 'pandas>=1.4.2', 'numpy==1.22.*', 'requests~=2.28.0', 'scipy>=1.6.3,<=1.8.1', 'torch==1.11.0',
     'prophecy-libs==1.3.4'],
    entry_points = {
'console_scripts' : [
'main = job.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
