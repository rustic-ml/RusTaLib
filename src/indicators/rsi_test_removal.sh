#!/bin/bash

# This script removes test modules from source files and moves them to the test directory
# Run this script from the root of the project

# Function to remove the test module from a file
remove_test_module() {
    local file=$1
    # Check if file exists and contains a test module
    if [ -f "$file" ] && grep -q "#\[cfg(test)\]" "$file"; then
        # Create a temporary file
        local tmpfile=$(mktemp)
        
        # Extract everything before the test module
        sed -n '1,/#\[cfg(test)\]/!d' "$file" > "$tmpfile"
        
        # Find the end of the test module (closing brace of the module)
        local start_line=$(grep -n "#\[cfg(test)\]" "$file" | cut -d: -f1)
        local file_length=$(wc -l < "$file")
        local in_test_module=false
        local brace_count=0
        
        for ((i=$start_line; i<=$file_length; i++)); do
            local line=$(sed -n "${i}p" "$file")
            
            if [[ "$line" == *"mod tests"* ]] || [[ "$line" == *"mod integration_tests"* ]]; then
                in_test_module=true
            fi
            
            if $in_test_module; then
                if [[ "$line" == *"{"* ]]; then
                    ((brace_count++))
                fi
                if [[ "$line" == *"}"* ]]; then
                    ((brace_count--))
                    if [ $brace_count -eq 0 ]; then
                        # Found the end of the test module
                        in_test_module=false
                        break
                    fi
                fi
            fi
        done
        
        # Extract everything after the test module
        sed -n "$((i+1)),\$p" "$file" >> "$tmpfile"
        
        # Replace the original file
        mv "$tmpfile" "$file"
        echo "Removed test module from $file"
    fi
}

# Process RSI file
remove_test_module "src/indicators/oscillators/rsi.rs"

# You can add more files here or use find to process all files
# find src -name "*.rs" -exec bash -c 'remove_test_module "$0"' {} \; 