import os, sys, importlib.util
import importlib
import importlib.util
import inspect
from pathlib import Path
from typing import Dict, Type, Optional
from .base_calculator import BaseIndicatorCalculator
from ...tools import info, error, warning


class CalculatorLoader:
    """
    Automatically discovers and loads indicator calculators from the calculators directory.
    
    This class implements the Module Discovery pattern to automatically find and register
    all indicator calculators without requiring manual registration.
    """
    
    _calculators: Dict[str, Type[BaseIndicatorCalculator]] = {}
    _loaded: bool = False

    @classmethod
    def load_all(cls) -> None:
        """
        Auto-discover and load all indicator calculators from the calculators directory.
        
        This method scans the calculators directory for Python files and automatically
        imports and registers any classes that inherit from BaseIndicatorCalculator.
        """
        if cls._loaded:
            return
            
        info("Starting auto-discovery of indicator calculators",
             component="calculator_loader")
        
        try:
            # Get the calculators directory path
            calculators_dir = Path(__file__).parent / "calculators"
            
            if not calculators_dir.exists():
                warning(f"Calculators directory not found {str(calculators_dir)}",
                       component="calculator_loader",
                       expected_path=str(calculators_dir))
                cls._loaded = True
                return
            
            # Scan for Python files in calculators directory
            calculator_files = list(calculators_dir.glob("*.py"))
            
            info(f"Found {len(calculator_files)} potential calculator files",
                 component="calculator_loader",
                 files=[f.name for f in calculator_files])
            
            loaded_count = 0
            failed_count = 0
            
            for py_file in calculator_files:
                if py_file.name.startswith("_"):
                    continue  # Skip private files like __init__.py
                
                try:
                    cls._load_calculator_from_file(py_file)
                    loaded_count += 1
                    
                except Exception as e:
                    error(f"Failed to load calculator from {py_file.name}: {str(e)}",
                          component="calculator_loader",
                          file=py_file.name,
                          error=str(e))
                    failed_count += 1
                    continue

            info(f"Calculator discovery completed: {loaded_count} loaded, {failed_count} failed",
                 component="calculator_loader",
                 total_loaded=loaded_count,
                 failed=failed_count,
                 registered_indicators=list(cls._calculators.keys()))
            
            cls._loaded = True
            
        except Exception as e:
            error(f"Failed to load calculators error: {str(e)}",
                  component="calculator_loader",
                  error=str(e))
            cls._loaded = True  # Prevent infinite retry

    @classmethod
    def _load_calculator_from_file(cls, py_file: Path) -> None:
        """
        Load calculator classes from a specific Python file.
        
        Args:
            py_file: Path to the Python file to load
        """
        module_name = py_file.stem
        
        try:
            root = Path(__file__).resolve().parents[3]  # .../src
            sys.path.append(str(root))

            relative_path = py_file.relative_to(root)
            module_name = ".".join(relative_path.with_suffix("").parts)

            spec = importlib.util.spec_from_file_location(module_name, py_file)
            if spec is None or spec.loader is None:
                warning(f"Could not create module spec for {py_file.name}",
                       component="calculator_loader")
                return

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # Find calculator classes in the module
            calculators_found = 0
            
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a calculator class using duck typing
                try:
                    # Skip non-calculator classes
                    if not name.endswith('Calculator'):
                        continue
                    
                    # Skip imported BaseIndicatorCalculator itself
                    if name == 'BaseIndicatorCalculator':
                        continue
                        
                    # Check required attributes (duck typing approach)
                    if (hasattr(obj, 'INDICATOR_CODE') and 
                        obj.INDICATOR_CODE is not None and
                        hasattr(obj, 'SUPPORTED_TEMPORALITIES')):
                        
                        # Validate the calculator class
                        if cls._validate_calculator_class(obj):
                            indicator_code = obj.INDICATOR_CODE.upper()
                            
                            if indicator_code in cls._calculators:
                                warning(f"Indicator {indicator_code} already registered, skipping",
                                       component="calculator_loader",
                                       existing_class=cls._calculators[indicator_code].__name__,
                                       new_class=obj.__name__)
                                continue
                            
                            cls._calculators[indicator_code] = obj
                            calculators_found += 1
                            
                            info(f"Registered calculator for indicator {indicator_code}",
                                 component="calculator_loader",
                                 indicator_code=indicator_code,
                                 class_name=obj.__name__,
                                 supported_temporalities=getattr(obj, 'SUPPORTED_TEMPORALITIES', []))
                except Exception as validation_error:
                    # Skip classes that fail validation
                    continue
            
            if calculators_found == 0:
                warning(f"No valid calculator classes found in {py_file.name}",
                       component="calculator_loader")
                
        except Exception as e:
            error(f"Error loading module {module_name}",
                  component="calculator_loader",
                  module_name=module_name,
                  error=str(e))
            raise

    @classmethod
    def _validate_calculator_class(cls, calculator_class: Type) -> bool:
        """
        Validate that a calculator class meets the requirements.
        
        Args:
            calculator_class: The calculator class to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Check required attributes
            if not hasattr(calculator_class, 'INDICATOR_CODE'):
                warning(f"Calculator {calculator_class.__name__} missing INDICATOR_CODE",
                       component="calculator_loader")
                return False
            
            if not hasattr(calculator_class, 'SUPPORTED_TEMPORALITIES'):
                warning(f"Calculator {calculator_class.__name__} missing SUPPORTED_TEMPORALITIES",
                       component="calculator_loader")
                return False
            
            if not calculator_class.SUPPORTED_TEMPORALITIES:
                warning(f"Calculator {calculator_class.__name__} has empty SUPPORTED_TEMPORALITIES",
                       component="calculator_loader")
                return False
            
            # Check that required methods exist for supported temporalities
            for temporality in calculator_class.SUPPORTED_TEMPORALITIES:
                method_name = f"calculate_{temporality}"
                if not hasattr(calculator_class, method_name):
                    warning(f"Calculator {calculator_class.__name__} missing method {method_name}",
                           component="calculator_loader",
                           indicator_code=calculator_class.INDICATOR_CODE,
                           temporality=temporality)
                    return False
            
            return True
            
        except Exception as e:
            error(f"Error validating calculator class {calculator_class.__name__}",
                  component="calculator_loader",
                  error=str(e))
            return False

    @classmethod
    def get_calculator(cls, indicator_code: str) -> Optional[Type[BaseIndicatorCalculator]]:
        """
        Get a calculator class for the specified indicator code.
        
        Args:
            indicator_code: The indicator short name/code (e.g., "TXX")
            
        Returns:
            Optional[Type[BaseIndicatorCalculator]]: Calculator class if found, None otherwise
        """
        # Ensure calculators are loaded
        if not cls._loaded:
            cls.load_all()
        
        calculator_class = cls._calculators.get(indicator_code.upper())
        
        if calculator_class:
            info(f"Found calculator for indicator {indicator_code}",
                 component="calculator_loader",
                 indicator_code=indicator_code,
                 calculator_class=calculator_class.__name__)
        else:
            warning(f"No calculator found for indicator {indicator_code}",
                   component="calculator_loader",
                   indicator_code=indicator_code,
                   available_indicators=list(cls._calculators.keys()))
        
        return calculator_class

    @classmethod
    def get_available_indicators(cls) -> Dict[str, Type[BaseIndicatorCalculator]]:
        """
        Get all available indicator calculators.
        
        Returns:
            Dict[str, Type[BaseIndicatorCalculator]]: Mapping of indicator codes to calculator classes
        """
        if not cls._loaded:
            cls.load_all()
        
        return cls._calculators.copy()

    @classmethod
    def reload(cls) -> None:
        """
        Force reload of all calculators (useful for development/testing).
        """
        info("Forcing reload of all calculators",
             component="calculator_loader")
        
        cls._calculators.clear()
        cls._loaded = False
        cls.load_all()

    @classmethod
    def is_indicator_supported(cls, indicator_code: str, temporality: str) -> bool:
        """
        Check if an indicator supports a specific temporality.
        
        Args:
            indicator_code: The indicator short name/code
            temporality: The temporality to check (e.g., "annual", "monthly")
            
        Returns:
            bool: True if supported, False otherwise
        """
        calculator_class = cls.get_calculator(indicator_code)
        if not calculator_class:
            return False
        
        return temporality in calculator_class.SUPPORTED_TEMPORALITIES