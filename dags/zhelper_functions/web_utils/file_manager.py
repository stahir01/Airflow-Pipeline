import re
from pathlib import Path
from typing import Optional
import logging

class FilePathManager:
    """
    Enhanced file path manager with automatic versioning based on existing files.
    Organizes files in project-specific directories with incremental version numbers.
    """
    
    @staticmethod
    def sanitize_directory_name(name: str) -> str:
        return re.sub(r'[^\w\-_]', '_', name).strip('_')
    
    @staticmethod
    def extract_version_number(filename: str) -> Optional[int]:
        """
        Extract version number from filename (e.g., 'file_v2.txt' → 2)
        Returns None if no version number found
        """
        match = re.search(r'_v(\d+)(?:\..+)?$', filename.stem)
        return int(match.group(1)) if match else None
    
    @staticmethod
    def get_latest_version(project_dir: Path, base_name: str) -> int:
        """
        Find the highest version number for files matching the base name pattern
        """
        max_version = 0
        pattern = re.compile(rf'^{re.escape(base_name)}_v(\d+)(?:\..+)?$')
        
        for file in project_dir.iterdir():
            if file.is_file():
                match = pattern.match(file.stem)
                if match:
                    version = int(match.group(1))
                    if version > max_version:
                        max_version = version
        return max_version

    @staticmethod
    def get_project_filepath(
        project_name: str,
        file_name: str,
        dataset_path: Path = Path("data"),
        max_attempts: int = 100
    ) -> Path:
        """
        Get a versioned file path within the project directory.
        Automatically increments version number based on existing files.
        
        Args:
            project_name: Name of the project/organization
            file_name: Desired filename (with extension)
            dataset_path: Root directory for all datasets
            max_attempts: Maximum number of versioning attempts
            
        Returns:
            Path: Versioned file path that can be safely written to
        """
        # Sanitize and create project directory
        sanitized_project = FilePathManager.sanitize_directory_name(project_name)
        project_dir = dataset_path / sanitized_project
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare filename components
        file_path = Path(file_name)
        base_name = file_path.stem
        suffix = file_path.suffix
        
        # Find the latest existing version
        latest_version = FilePathManager.get_latest_version(project_dir, base_name)
        next_version = latest_version + 1
        
        # Handle first version case
        if next_version == 1:
            # Check if unversioned file exists
            unversioned_path = project_dir / file_name
            if not unversioned_path.exists():
                logging.info(f"Using unversioned path: {unversioned_path}")
                return unversioned_path
                
        # Generate versioned filename
        versioned_filename = f"{base_name}_v{next_version}{suffix}"
        versioned_path = project_dir / versioned_filename
        

        logging.info(f"Using versioned path: {versioned_path}")
        return versioned_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test cases
    test_dir = Path("dataset")
    test_dir.mkdir(exist_ok=True)
    
    
    # Test 1: First file (unversioned)
    path1 = FilePathManager.get_project_filepath(
        "test_project", "document.md", test_dir
    )
    print(f"File Path: {path1}")  # test_data/test_project/document.md
    
    # Test 2: Version 2
    path2 = FilePathManager.get_project_filepath(
        "test_project", "document.md", test_dir
    )
    print(f"File Path: {path2}")  # test_data/test_project/document_v2.md
    
    # Test 3: Different filename
    path3 = FilePathManager.get_project_filepath(
        "test_project", "config.json", test_dir
    )
    print(f"File Path: {path3}")  # test_data/test_project/config.json