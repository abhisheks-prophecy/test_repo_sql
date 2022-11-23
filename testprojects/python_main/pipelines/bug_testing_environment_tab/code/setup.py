from setuptools import setup, find_packages
setup(
    name = 'bug_testing_environment_tab',
    version = '1.0',
    packages = find_packages(include = ('bug_testing_environment_tab*', )) + ["prophecy_config_instances"],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.3.11'],
    entry_points = {
'console_scripts' : [
'main = bug_testing_environment_tab.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
