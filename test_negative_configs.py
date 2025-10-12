#!/usr/bin/env python3
"""
Test script to validate negative test configuration files.

This script tests the validation logic without running the full pipeline.
It's useful for:
1. Verifying that test config files are correctly formatted
2. Confirming expected validation errors occur
3. Testing validation logic changes

Usage:
    python test_negative_configs.py
    python test_negative_configs.py --config sample_dq_config_negative_tests.json
    python test_negative_configs.py --verbose
"""

import json
import sys
import argparse
from typing import List, Dict, Tuple
from validation import ValidationEngine
from exceptions import ValidationError


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def validate_config_file(config_file: str, verbose: bool = False) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a configuration file and return valid checks and errors.
    
    Args:
        config_file: Path to JSON configuration file
        verbose: Whether to print verbose output
        
    Returns:
        Tuple of (valid_checks, validation_errors)
    """
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Testing Configuration File{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"File: {config_file}\n")
    
    # Load JSON file
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    except FileNotFoundError:
        print(f"{Colors.RED}❌ ERROR: File not found: {config_file}{Colors.END}")
        return [], []
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}❌ ERROR: Invalid JSON format: {e}{Colors.END}")
        return [], []
    except Exception as e:
        print(f"{Colors.RED}❌ ERROR: Failed to load file: {e}{Colors.END}")
        return [], []
    
    if not isinstance(config_data, list):
        print(f"{Colors.RED}❌ ERROR: Config must be a list of checks{Colors.END}")
        return [], []
    
    print(f"Total checks in file: {len(config_data)}\n")
    
    # Validate each check
    valid_checks = []
    validation_errors = []
    
    for index, check in enumerate(config_data):
        check_id = check.get('check_id', f'Unknown_{index}')
        
        try:
            # Validate this check
            ValidationEngine.validate_dq_record(check, index)
            valid_checks.append(check)
            
            if verbose:
                print(f"{Colors.GREEN}✅ Check {index + 1}/{len(config_data)}: {check_id} - VALID{Colors.END}")
            
        except ValidationError as e:
            error_info = {
                'index': index,
                'check_id': check_id,
                'error': str(e),
                'check_config': check
            }
            validation_errors.append(error_info)
            
            print(f"{Colors.RED}❌ Check {index + 1}/{len(config_data)}: {check_id} - VALIDATION FAILED{Colors.END}")
            print(f"   Error: {e}\n")
    
    return valid_checks, validation_errors


def print_summary(config_file: str, valid_checks: List[Dict], validation_errors: List[Dict]):
    """Print validation summary."""
    total = len(valid_checks) + len(validation_errors)
    
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Validation Summary{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"File: {config_file}")
    print(f"Total checks: {total}")
    print(f"{Colors.GREEN}Valid checks: {len(valid_checks)}{Colors.END}")
    print(f"{Colors.RED}Invalid checks: {len(validation_errors)}{Colors.END}")
    
    if total > 0:
        success_rate = (len(valid_checks) / total * 100)
        color = Colors.GREEN if success_rate == 100 else Colors.YELLOW if success_rate >= 50 else Colors.RED
        print(f"{color}Success rate: {success_rate:.1f}%{Colors.END}")
    
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")


def print_error_details(validation_errors: List[Dict], verbose: bool = False):
    """Print detailed error information."""
    if not validation_errors:
        print(f"{Colors.GREEN}✅ All checks validated successfully!{Colors.END}\n")
        return
    
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Validation Errors Details{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")
    
    for i, error in enumerate(validation_errors, 1):
        print(f"{Colors.YELLOW}{'-'*80}{Colors.END}")
        print(f"{Colors.BOLD}Error #{i}{Colors.END}")
        print(f"{Colors.YELLOW}{'-'*80}{Colors.END}")
        print(f"Check ID: {Colors.BOLD}{error['check_id']}{Colors.END}")
        print(f"Index: {error['index']}")
        print(f"Error: {Colors.RED}{error['error']}{Colors.END}")
        
        if verbose:
            print(f"\nCheck Configuration:")
            print(json.dumps(error['check_config'], indent=2))
        
        print()


def test_expected_errors(config_file: str, expected_invalid_count: int, expected_valid_count: int) -> bool:
    """
    Test that a config file has the expected number of valid and invalid checks.
    
    Args:
        config_file: Path to config file
        expected_invalid_count: Expected number of invalid checks
        expected_valid_count: Expected number of valid checks
        
    Returns:
        True if expectations match, False otherwise
    """
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Testing Expected Error Counts{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"File: {config_file}")
    print(f"Expected valid: {expected_valid_count}")
    print(f"Expected invalid: {expected_invalid_count}\n")
    
    valid_checks, validation_errors = validate_config_file(config_file, verbose=False)
    
    valid_match = len(valid_checks) == expected_valid_count
    invalid_match = len(validation_errors) == expected_invalid_count
    
    print(f"\n{Colors.BOLD}Results:{Colors.END}")
    
    if valid_match:
        print(f"{Colors.GREEN}✅ Valid checks: {len(valid_checks)} (expected {expected_valid_count}){Colors.END}")
    else:
        print(f"{Colors.RED}❌ Valid checks: {len(valid_checks)} (expected {expected_valid_count}){Colors.END}")
    
    if invalid_match:
        print(f"{Colors.GREEN}✅ Invalid checks: {len(validation_errors)} (expected {expected_invalid_count}){Colors.END}")
    else:
        print(f"{Colors.RED}❌ Invalid checks: {len(validation_errors)} (expected {expected_invalid_count}){Colors.END}")
    
    success = valid_match and invalid_match
    
    if success:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✅ TEST PASSED{Colors.END}")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ TEST FAILED{Colors.END}")
    
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")
    
    return success


def run_all_tests():
    """Run tests on all negative test configuration files."""
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Running All Negative Test Configuration Tests{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")
    
    tests = [
        {
            'file': 'sample_dq_config_negative_tests.json',
            'expected_valid': 18,
            'expected_invalid': 2,
            'description': 'Mixed scenario (18 valid, 2 invalid)'
        },
        {
            'file': 'sample_dq_config_all_negative_scenarios.json',
            'expected_valid': 0,
            'expected_invalid': 20,
            'description': 'All error scenarios (0 valid, 20 invalid)'
        }
    ]
    
    results = []
    
    for test in tests:
        print(f"\n{Colors.BLUE}{Colors.BOLD}Test: {test['description']}{Colors.END}")
        success = test_expected_errors(
            test['file'],
            test['expected_invalid'],
            test['expected_valid']
        )
        results.append({
            'file': test['file'],
            'description': test['description'],
            'success': success
        })
    
    # Print overall summary
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Overall Test Results{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")
    
    passed = sum(1 for r in results if r['success'])
    total = len(results)
    
    for result in results:
        status = f"{Colors.GREEN}✅ PASS{Colors.END}" if result['success'] else f"{Colors.RED}❌ FAIL{Colors.END}"
        print(f"{status} - {result['description']}")
        print(f"       File: {result['file']}\n")
    
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}✅ ALL TESTS PASSED{Colors.END}")
        return 0
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ SOME TESTS FAILED{Colors.END}")
        return 1


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Test negative configuration files for DQ pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests
  python test_negative_configs.py
  
  # Test specific config file
  python test_negative_configs.py --config sample_dq_config_negative_tests.json
  
  # Test with verbose output
  python test_negative_configs.py --config sample_dq_config_negative_tests.json --verbose
  
  # Test expected error counts
  python test_negative_configs.py --config sample_dq_config_negative_tests.json --expect-valid 18 --expect-invalid 2
        """
    )
    
    parser.add_argument(
        '--config',
        help='Path to specific config file to test'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print verbose output including all check validations'
    )
    parser.add_argument(
        '--expect-valid',
        type=int,
        help='Expected number of valid checks'
    )
    parser.add_argument(
        '--expect-invalid',
        type=int,
        help='Expected number of invalid checks'
    )
    
    args = parser.parse_args()
    
    # If no config specified, run all tests
    if not args.config:
        return run_all_tests()
    
    # Test specific config file
    if args.expect_valid is not None and args.expect_invalid is not None:
        # Test with expected counts
        success = test_expected_errors(args.config, args.expect_invalid, args.expect_valid)
        return 0 if success else 1
    else:
        # Just validate and show results
        valid_checks, validation_errors = validate_config_file(args.config, args.verbose)
        print_summary(args.config, valid_checks, validation_errors)
        print_error_details(validation_errors, args.verbose)
        
        return 1 if validation_errors else 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Test interrupted by user{Colors.END}")
        sys.exit(130)
    except Exception as e:
        print(f"\n{Colors.RED}❌ Unexpected error: {e}{Colors.END}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
