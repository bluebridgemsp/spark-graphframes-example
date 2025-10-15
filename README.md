# Sample Spark + GraphFrames Application

This repository provides a simple example of how to set up and run a Spark application using GraphFrames.

## Prerequisites

- Python 3.13 or higher
- JDK 21

## Setting Up the Environment

Follow these steps to create a virtual environment with the necessary dependencies.

### Using `uv`

If you have `uv` installed, set up the environment as follows:

1. Create a virtual environment:

    ```bash
    uv env
    ```

2. Install dependencies:

    ```bash
    uv sync
    ```

3. Activate the virtual environment:

    - On Unix or macOS:

      ```bash
      source .venv/bin/activate
      ```

    - On Windows:

      ```bash
      .\.venv\Scripts\activate
      ```

### Using `venv` and `pip`

If you don't have `uv`, you can manually create a virtual environment and install dependencies:

1. Create a virtual environment:

    ```bash
    python3 -m venv .venv
    ```

2. Activate the virtual environment:

    - On Unix or macOS:

      ```bash
      source .venv/bin/activate
      ```

    - On Windows:

      ```bash
      .\.venv\Scripts\activate
      ```

3. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Running the Application

To run the Spark application, use the following command:

```bash
python app.py
```
