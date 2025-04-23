Certainly! Below is the updated `docs/contributing/index.md` for your `rustvello` project. This version includes detailed information about **Labels** and the **Conventional Commit** format for PRs and commits. This will help maintain consistency, streamline contributions, and ensure clarity in the contribution process once the project is open for external contributions.

---

# Contributing to rustvello

Thank you for your interest in contributing to rustvello! At this moment, the project is in its initial development phase and is not yet open for external contributions. We are diligently working towards a Minimum Viable Product (MVP) and establishing a stable foundation.

Once the project reaches a stage where community contributions can be integrated, this guide will be updated with detailed instructions on the contribution process. For now, we invite you to watch the repository for updates and participate in issue discussions.

We appreciate your understanding and look forward to your contributions in the future!

Best regards,

The rustvello Team

---

## Labels

We use a set of standardized labels to categorize issues and pull requests. Proper labeling helps in tracking progress, prioritizing tasks, and organizing contributions effectively.

### **Using Labels**

- **Issues:** When reporting or addressing an issue, apply the relevant labels to categorize the type of issue (e.g., bug, feature request).
- **Pull Requests:** Ensure your PRs are labeled appropriately based on the nature of the changes (e.g., fix, feature).

### **Syncing Labels**

Our workflow automatically syncs labels from the `.github/labels.yml` configuration. If you need to add or modify labels, please update the `labels.yml` file accordingly and submit a PR.

---

## Commit and PR Message Guidelines

To maintain a clean and understandable project history, we adhere to the **Conventional Commit** format for all commit messages and PR titles. This standardization facilitates automated tooling and ensures clarity in the purpose of changes.

### **Conventional Commit Format**

Each commit message should follow this structure:

```plaintext
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

#### **Conventional Commit Format Components:**

- **`<type>`**: Describes the category of the commit. Must be one of the following:

  - `feat`: A new feature.
  - `fix`: A bug fix.
  - `docs`: Documentation only changes.
  - `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc.).
  - `refactor`: A code change that neither fixes a bug nor adds a feature.
  - `perf`: A code change that improves performance.
  - `test`: Adding missing tests or correcting existing tests.
  - `build`: Changes that affect the build system or external dependencies.
  - `ci`: Changes to our CI configuration files and scripts.
  - `chore`: Other changes that don't modify src or test files.
  - `revert`: Reverts a previous commit.

- **`<scope>`**: _(Optional)_ A noun describing a section of the codebase enclosed in parentheses. For example, `feat(auth): add login functionality`.

- **`<description>`**: A short, imperative description of the change.

#### **Conventional Commit Format Examples:**

- **Feature Addition:**

  ```plaintext
  feat(auth): add login functionality
  ```

- **Bug Fix:**

  ```plaintext
  fix(api): handle null responses gracefully
  ```

- **Documentation Update:**

  ```plaintext
  docs(readme): update installation instructions
  ```

- **Refactoring Code:**

  ```plaintext
  refactor(utils): simplify error handling
  ```

- **Chore:**

  ```plaintext
  chore(deps): update dependency lodash to v4.17.21
  ```

### **Pull Request Titles**

Your PR title should mirror your commit message format for consistency.

#### **Format:**

```plaintext
<type>(<scope>): <description>
```

#### **Pull Request Titles Examples:**

- **Feature PR:**

  ```plaintext
  feat(auth): implement OAuth2 authentication
  ```

- **Bug Fix PR:**

  ```plaintext
  fix(ui): resolve button alignment issue on mobile
  ```

- **Documentation PR:**

  ```plaintext
  docs(api): add examples for new endpoints
  ```

### **Guidelines for Writing Commit Messages and PR Titles**

1. **Be Clear and Concise:**
   Ensure that your descriptions accurately reflect the changes made.

2. **Use the Imperative Mood:**
   Start your descriptions with a verb (e.g., "Add", "Fix", "Update").

3. **Reference Issues and PRs:**
   If your commit or PR relates to an existing issue, reference it in the footer using `Closes #<issue-number>` or `Fixes #<issue-number>`.

4. **Avoid Subjective Language:**
   Stick to objective descriptions that describe the "what" and "why" of the changes.

---

## Labels and Automation

Our project utilizes automated workflows to manage labels and releases. Understanding how these systems work will help you contribute more effectively.

### **Label Management**

We use GitHub Actions to automatically sync and manage labels based on our `.github/labels.yml` configuration. This ensures consistency across all issues and pull requests.

- **Syncing Labels:**
  The `crazy-max/ghaction-github-labeler@v5` action syncs labels defined in `.github/labels.yml` with your GitHub repository. Ensure that any new labels you introduce are added to this configuration file.

- **Applying Labels Based on PR Titles:**
  Pull requests are automatically labeled based on their titles using the `bcoe/conventional-release-labels@v1` action. Ensure your PR titles follow the **Conventional Commit** format to benefit from this automation.

### **Release Management**

We use **Release Drafter** to automate the creation of release notes based on merged pull requests.

- **Release Drafter Configuration:**
  The configuration file `.github/release-drafter.yml` defines how release notes are generated, categorizing changes based on labels.

- **Automated Releases:**
  When changes are pushed to the `main` branch, the `release-drafter/release-drafter@v5` action generates or updates a draft release, aggregating changes from merged PRs.

- **Publishing Packages:**
  Our CI/CD pipeline handles publishing Rust crates and Python packages. It includes checks to prevent publishing duplicate versions and ensures that releases are only created when new versions are available.

---

## Handling Labeling and Release Errors

### **Release Drafter Errors**

If you encounter errors related to Release Drafter, such as missing configuration files or invalid templates:

1. **Ensure Configuration File Exists:**
   Verify that `.github/release-drafter.yml` exists in the default branch and is correctly formatted.

2. **Validate YAML Syntax:**
   Use a YAML linter to ensure there are no syntax errors in your configuration.

3. **Use Supported Placeholders:**
   Ensure your `template` uses valid placeholders like `$CHANGES`, `$CATEGORIES`, and `$CONTRIBUTORS`.

### **Sync Labels Errors**

If syncing labels fails due to incorrect color codes or invalid configurations:

1. **Quote Color Codes:**
   Ensure all color codes in `.github/labels.yml` are quoted strings, especially if they are purely numeric.

   ```yaml
   - name: chore
     description: Maintenance tasks
     color: "999999"
   ```

2. **Include All Managed Labels:**
   Add commonly used labels like `good first issue` and `help wanted` to your `labels.yml` to prevent exclusion warnings.

   ```yaml
   - name: good first issue
     description: Good for newcomers
     color: "7057ff"
   - name: help wanted
     description: Extra attention is needed
     color: "008672"
   ```

3. **Review Workflow Permissions:**
   Ensure your GitHub Actions workflows have the necessary permissions to manage labels and issues.

---

## Setting Up the Development Environment

To contribute to rustvello once it's open for contributions, you can prepare your development environment by following these typical steps:

1. **Fork the Repository**: Start by forking the rustvello repository (<https://github.com/your-username/rustvello>) on GitHub to your own account.

2. **Clone the Fork**: Clone your fork locally.

   ```bash
   cd <directory_in_which_repo_should_be_created>
   git clone git@github.com:YOUR_NAME/rustvello.git
   ```

3. **Navigate into the directory:**

   ```bash
   cd rustvello
   ```

4. **Install Dependencies**:

   - **Docker**: Make sure you have Docker installed on your system as it may be used for running services such as databases or other dependencies.

     - Docker Installation: Visit <https://docs.docker.com/get-docker/>

   - **Poetry**: rustvello uses Poetry for dependency management. Install Poetry using the recommended method from the official documentation at <https://python-poetry.org/docs/#installation>.

5. **Set Up the Project**: Inside the project directory, set up your local development environment using Poetry. This will install all dependencies, including those needed for development.

   ```bash
   poetry install
   ```

6. **Install Pre-commit Hooks**: After installing all dependencies, set up pre-commit hooks in your local repository. This ensures that code quality checks are automatically performed before each commit.

   ```bash
   poetry run pre-commit install
   ```

7. **Activate the Virtual Environment**: Use Poetry to activate the virtual environment.

   ```bash
   poetry shell
   ```

8. **Start Development**: You are now ready to start exploring the codebase. While we are not accepting contributions yet, you can familiarize yourself with the project structure and code.

   ```{attention}
   The Python docstrings will be rendered by MyST and `autodoc2`. Use Markdown to document them and any MyST formatting allowed. For examples, check the [MyST syntax cheat sheet](https://jupyterbook.org/en/stable/reference/cheatsheet.html), [Roles and Directives](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html), and the complete directives references at [MyST docs](https://mystmd.org/guide/directives).
   ```

---

## Running Tests and Checking Coverage

rustvello aims to maintain a high standard of code quality, which includes thorough testing and maintaining good test coverage. Here’s how you can run tests and check coverage:

1. **Running Tests**: After setting up your development environment, you can run the tests to ensure everything is working as expected.

   ```bash
   poetry run pytest
   ```

   This command will execute all the tests in the `tests` directory.

2. **Checking Test Coverage**: To check how much of the code is covered by tests, use the `coverage` tool.

   - First, run the tests with coverage tracking:

     ```bash
     poetry run coverage run -m pytest
     ```

   - Then, generate a coverage report. There are two ways to view the coverage report:

     - For a summary in the console, use:

       ```bash
       poetry run coverage report
       ```

     - For a more detailed HTML report, use:

       ```bash
       poetry run coverage html
       ```

       This will generate a report in the `htmlcov` directory. You can open `htmlcov/index.html` in a web browser to view it.

Please aim to maintain or improve the test coverage with your contributions. It’s recommended to add tests for any new code or when fixing bugs.

---

## License

Any contributions you make to this project will fall under the [MIT License](https://github.com/your-username/rustvello/blob/main/LICENSE) that covers the rustvello project.

---

```{toctree}
:hidden:
:maxdepth: 2
:caption: Detailed Guides

ide
test
```
