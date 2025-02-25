# Contributing to Mage.ai

This guide explains how to set up Mage.ai locally, make changes to the content in the backend folder, and submit those changes via pull requests.

## Table of Contents

1. [Local Development Setup](#local-development-setup)
2. [Making Changes](#making-changes)
3. [Testing Your Changes](#testing-your-changes)
4. [Submitting a Pull Request](#submitting-a-pull-request)
5. [Best Practices](#best-practices)

## Local Development Setup

### Prerequisites

- Docker installed on your machine
- Git installed on your machine
- Access to the repository

### Setting Up Mage.ai Locally

1. Clone the repository:
   ```bash
   git clone https://github.com/Klimabevaegelsen/landbruget.dk.git
   cd landbruget.dk
   ```

2. Create a new branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. Start Mage.ai using Docker:
   ```bash
   docker run -it -p 6789:6789 -v $(pwd)/backend/mage:/home/src mageai/mageai
   ```

4. Access the Mage.ai UI in your browser at http://localhost:6789

## Making Changes

### Project Structure

The Mage.ai project is located in the `backend/mage` directory with the following structure:

- `data_loaders/`: Contains data loader blocks
- `transformers/`: Contains transformer blocks
- `data_exporters/`: Contains data exporter blocks
- `pipelines/`: Contains pipeline definitions
- `custom/`: Contains custom code and functions
- `utils/`: Contains utility functions
- `io_config.yaml`: Configuration for data sources and destinations
- `metadata.yaml`: Project metadata and configuration

### Creating or Modifying Pipelines

1. In the Mage.ai UI, navigate to "Pipelines" in the left sidebar
2. To create a new pipeline, click "New pipeline"
3. To modify an existing pipeline, click on the pipeline name

### Creating or Modifying Blocks

1. In a pipeline, click "Add block" to create a new block
2. Select the appropriate block type (data loader, transformer, or data exporter)
3. Write your code in the block editor
4. Click "Save and run" to test your changes

### Adding Custom Code

1. Create or modify files in the `custom/` directory
2. Import your custom code in blocks using:
   ```python
   from mage_ai.data_preparation.repo_manager import get_repo_path
   from os import path
   import sys

   repo_path = get_repo_path()
   sys.path.append(repo_path)

   from custom.your_module import your_function
   ```

## Testing Your Changes

1. Test your changes in the Mage.ai UI by running the affected blocks and pipelines
2. Verify that your changes produce the expected results
3. Check for any errors or warnings in the logs

## Submitting a Pull Request

1. Commit your changes:
   ```bash
   git add backend/mage/
   git commit -m "Description of your changes"
   ```

2. Push your branch to the remote repository:
   ```bash
   git push origin feature/your-feature-name
   ```

3. Create a pull request on GitHub:
   - Navigate to the repository on GitHub
   - Click "Pull requests" and then "New pull request"
   - Select your branch as the compare branch
   - Add a descriptive title and detailed description of your changes
   - Request reviews from appropriate team members
   - Submit the pull request

## Best Practices

1. **Keep changes focused**: Each pull request should address a single concern
2. **Document your code**: Add comments to explain complex logic
3. **Follow naming conventions**: Use descriptive names for pipelines and blocks
4. **Reuse code**: Create utility functions in the `utils/` directory for reusable code
5. **Test thoroughly**: Ensure your changes work as expected before submitting a pull request
6. **Update documentation**: If your changes affect how others use the project, update the documentation

## Deployment

Once your pull request is approved and merged, the changes will be deployed to the production environment using the Terraform configuration in the `mage/terraform` directory.

For questions or assistance, please contact the project maintainers. 