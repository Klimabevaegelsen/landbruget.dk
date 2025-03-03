# Contributing to Mage.ai Pipelines

This guide provides step-by-step instructions for setting up your development environment and contributing to our Mage.ai pipelines. For technical documentation about the project architecture and implementation details, please refer to the [README.md](./README.md).

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Working with Pipelines](#working-with-pipelines)
3. [Best Practices](#best-practices)
4. [Testing Your Changes](#testing-your-changes)
5. [Submitting Changes](#submitting-changes)

## Development Environment Setup

### Prerequisites

- Docker installed
- Git installed
- Access to the repository

### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Klimabevaegelsen/landbruget.dk.git
   cd landbruget.dk
   ```

2. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Start Mage.ai with volume mounting**:
   ```bash
   docker run -it -p 6789:6789 -v $(pwd)/backend/mage:/home/src/default_repo mageai/mageai:latest
   ```

   > **Note**: This approach uses volume mounting, which allows you to make changes to pipeline files and see them immediately without rebuilding the container.

4. **Access the Mage.ai UI**:
   Open http://localhost:6789 in your browser.

## Working with Pipelines

### Creating or Modifying Pipelines

1. **Navigate to "Pipelines" in the UI**
2. **Create a new pipeline or select an existing one**:
   - For new pipelines: Click "New pipeline" and follow the wizard
   - For existing pipelines: Select from the list

3. **Add or modify blocks**:
   - **Data loaders**: For retrieving data from external sources
   - **Transformers**: For processing and transforming data
   - **Data exporters**: For storing processed data

### Adding Custom Code

You can import custom modules into your pipeline blocks:

```python
from mage_ai.data_preparation.repo_manager import get_repo_path
from os import path
import sys

repo_path = get_repo_path()
sys.path.append(repo_path)

from custom.your_module import your_function
```

### Managing Dependencies

If you need additional Python packages, add them to the `requirements.txt` file in the project root. For local development, you can install packages directly in the container:

```bash
pip install package-name
```

### Modifying Configuration

1. **Pipeline configuration**: Edit the `pipelines/[pipeline_name]/metadata.yaml` file
2. **Project configuration**: Edit the `metadata.yaml` file in the project root
3. **Data source configuration**: Edit the `io_config.yaml` file

## Best Practices

1. **Keep changes focused on single concerns**
   - Each PR should address a specific feature or bug

2. **Document complex logic**
   - Add comments for non-obvious code
   - Include docstrings for functions

3. **Use descriptive names**
   - Blocks should have clear, descriptive names
   - Variables should indicate their purpose

4. **Create reusable utility functions**
   - Place common code in the `utils/` directory
   - Import utilities across multiple blocks

5. **Follow memory management guidelines**
   - See the [Memory Management](./README.md#memory-management) section in README.md

6. **Implement proper error handling**
   - See the [Error Handling](./README.md#error-handling) section in README.md

## Testing Your Changes

1. **Run your pipeline**:
   - Click "Run Pipeline" in the UI
   - Check for any errors in the execution

2. **Use built-in tests**:
   - Add `@test` decorated functions to validate block outputs
   - Example:
     ```python
     @test
     def test_output(df):
         assert len(df) > 0, "DataFrame should not be empty"
         assert "required_column" in df.columns, "Required column missing"
     ```

3. **Monitor resource usage**:
   - Check memory usage during execution
   - Verify that large datasets are processed efficiently

## Submitting Changes

1. **Commit your changes**:
   ```bash
   git add backend/mage/
   git commit -m "Description of your changes"
   ```

2. **Push to your branch**:
   ```bash
   git push origin feature/your-feature-name
   ```

3. **Create a Pull Request on GitHub**:
   - Include a detailed description of your changes
   - Reference any related issues
   - Explain any technical decisions or trade-offs

4. **Respond to feedback**:
   - Address review comments
   - Make additional commits as needed
   - Re-request review once changes are made

## Deployment

For detailed information about how deployment works, see the [Deployment](./README.md#deployment) section in README.md.

---

Thank you for contributing to our Mage.ai pipelines! If you have any questions or need assistance, please reach out to the team. 