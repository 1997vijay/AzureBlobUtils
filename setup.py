import setuptools
setuptools.setup(
name='AzureUtils',
version='0.1',
author="Vijay Kumar",
author_email="vijay.kumar.1997@outlook.com",
description="Package can be used to intrect with Azure Storage account. It can be use to download to list file,containers, upload file,download files",
packages=['AzureUtils'],
install_requires=[
        'azure-storage-blob',
        'pandas',
    ],
classifiers=[
"Programming Language :: Python :: 3",
"License :: OSI Approved :: MIT License",
"Operating System :: OS Independent",
],
)