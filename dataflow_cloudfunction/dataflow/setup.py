import setuptools

setuptools.setup(
    name='example-dataflow-pipeline',
    version='0.0.1',
    description='Example pipeline package.',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "google-apitools==0.5.32",
        "google-cloud-core==2.2.3",
        "google-cloud-storage==1.44.0",
        "apache-beam==2.34.0"
    ],
 )


