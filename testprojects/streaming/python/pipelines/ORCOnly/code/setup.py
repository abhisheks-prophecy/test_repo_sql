from setuptools import setup, find_packages
setup(
    name = 'ORCOnly',
    version = '1.0',
    packages = find_packages(include = ('orconly*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.11.10'],
    entry_points = {
'console_scripts' : [
'main = orconly.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)