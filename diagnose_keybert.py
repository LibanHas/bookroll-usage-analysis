#!/usr/bin/env python
"""
Diagnostic script to verify KeyBERT installation and functionality.
This will help diagnose issues with the keyword extraction setup.

Usage:
    python diagnose_keybert.py
"""

import sys
import importlib
import traceback

def check_package(package_name):
    """Check if a package is installed and importable."""
    try:
        module = importlib.import_module(package_name)
        version = getattr(module, "__version__", "unknown")
        print(f"✅ {package_name} is installed (version: {version})")
        return True
    except ImportError as e:
        print(f"❌ {package_name} is NOT installed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error importing {package_name}: {e}")
        traceback.print_exc()
        return False

def test_torch():
    """Test PyTorch installation."""
    try:
        import torch
        print(f"✅ PyTorch is installed (version: {torch.__version__})")

        # Check CUDA availability (not required but good to know)
        print(f"   CUDA available: {torch.cuda.is_available()}")

        # Basic tensor operation to verify functionality
        x = torch.rand(5, 3)
        y = torch.rand(5, 3)
        z = x + y
        print(f"   PyTorch tensor operations work correctly")

        return True
    except Exception as e:
        print(f"❌ PyTorch test failed: {e}")
        traceback.print_exc()
        return False

def test_keybert():
    """Test KeyBERT functionality."""
    try:
        from keybert import KeyBERT
        print(f"✅ KeyBERT can be imported")

        # Initialize with a small model
        print("   Initializing KeyBERT model (this may take a moment)...")
        kw_model = KeyBERT("distilbert-base-nli-mean-tokens")

        # Test keyword extraction
        doc = "This is a test document about artificial intelligence and machine learning."
        keywords = kw_model.extract_keywords(doc)
        print(f"   KeyBERT successfully extracted keywords: {keywords}")

        return True
    except Exception as e:
        print(f"❌ KeyBERT test failed: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all diagnostic checks."""
    print("🔍 Starting KeyBERT diagnostics...")
    print("\n--- Package Installation Check ---")

    # Check core packages
    numpy_ok = check_package("numpy")
    torch_ok = check_package("torch")
    transformers_ok = check_package("transformers")
    sentence_transformers_ok = check_package("sentence_transformers")
    keybert_ok = check_package("keybert")

    # Test PyTorch functionality if installed
    print("\n--- PyTorch Functionality Check ---")
    if torch_ok:
        torch_test_ok = test_torch()
    else:
        torch_test_ok = False
        print("⚠️ Skipping PyTorch tests because it's not installed")

    # Test KeyBERT functionality if installed
    print("\n--- KeyBERT Functionality Check ---")
    if keybert_ok:
        keybert_test_ok = test_keybert()
    else:
        keybert_test_ok = False
        print("⚠️ Skipping KeyBERT tests because it's not installed")

    # Print summary
    print("\n--- Diagnostics Summary ---")
    all_ok = all([numpy_ok, torch_ok, transformers_ok, sentence_transformers_ok,
                 keybert_ok, torch_test_ok, keybert_test_ok])

    if all_ok:
        print("✅ All tests passed! KeyBERT is properly installed and functioning.")
    else:
        print("❌ Some tests failed. Check the output above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()