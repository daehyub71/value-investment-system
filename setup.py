from setuptools import setup, find_packages

setup(
    name="finance-data-vibe",
    version="1.0.0",
    description="워런 버핏 스타일 가치투자 시스템",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "streamlit>=1.28.0",
        "pandas>=1.5.0",
        "numpy>=1.24.0",
        "plotly>=5.15.0",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "TA-Lib>=0.4.25",
        "FinanceDataReader>=0.9.50",
        "python-dotenv>=1.0.0",
        "openpyxl>=3.1.0",
        "scikit-learn>=1.3.0",
    ],
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
