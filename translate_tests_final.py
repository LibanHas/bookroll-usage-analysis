#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re

def main():
    # Translation mappings with proper spacing
    translations = {
        # Compound terms first (longer phrases)
        'ベネッセ模試': 'Benesse Mock Test',
        '夏休み明けテスト': 'Post-Summer Break Test',
        '冬明けテスト': 'Post-Winter Test',
        '正の数・負の数': 'Positive and Negative Numbers',
        '文字の式': 'Algebraic Expressions',
        '変化と対応': 'Change and Correspondence',
        '平面図形': 'Plane Figures',
        '空間図形': 'Spatial Figures',
        'データの活用': 'Data Utilization',
        '式の計算': 'Expression Calculations',
        '連立方程式': 'Simultaneous Equations',
        '一次関数': 'Linear Functions',
        '図形の調べ方': 'Methods of Investigating Figures',
        '図形の性質と証明': 'Properties and Proofs of Figures',
        '箱ひげ図': 'Box-and-Whisker Plots',
        '展開・因数分解': 'Expansion and Factorization',
        '平方根': 'Square Roots',
        '二次方程式': 'Quadratic Equations',
        '二次関数': 'Quadratic Functions',
        '図形と相似': 'Figures and Similarity',
        '円の性質': 'Properties of Circles',
        '三平方の定理': 'Pythagorean Theorem',
        '土曜講座': 'Saturday Lecture',
        '学年末': 'End of Academic Year',
        '休校明け': 'Post-School Closure',
        '上書きテスト': 'Overwrite Test',
        'クラス番号': 'Class Number',
        '外進1': 'External Advancement 1',
        '外進2': 'External Advancement 2',
        '文基礎': 'Liberal Arts Basic',
        '文標準1': 'Liberal Arts Standard 1',
        '文標準2': 'Liberal Arts Standard 2',
        '理基礎': 'Science Basic',
        '理標準': 'Science Standard',
        '理発展': 'Science Advanced',
        '文発展': 'Liberal Arts Advanced',

        # Basic terms
        '内進': 'Internal Advancement',
        '外進': 'External Advancement',
        '理系': 'Science Track',
        '文系': 'Liberal Arts Track',
        '基礎': 'Basic',
        '標準': 'Standard',
        '発展': 'Advanced',
        '前期': 'First Semester',
        '後期': 'Second Semester',
        '中間': 'Midterm',
        '期末': 'Final',
        '数学': 'Mathematics',
        '英語': 'English',
        '国語': 'Japanese Language',
        '中学': 'Middle School',
        '高校': 'High School',
        '高2': 'High School Grade 2',
        '高1': 'High School Grade 1',
        '年度': 'Academic Year',
        '模試': 'Mock Test',
        '第1回': '1st Round',
        '第2回': '2nd Round',
        '1回': '1st Round',
        '2回': '2nd Round',
        'Bライン': 'B-Line',
        '章': 'Chapter',
        '方程式': 'Equations',
        '不等式': 'Inequalities',
        '確率': 'Probability',
        '数A': 'Math A',
        '数Ⅰ': 'Math I',
        '数Ⅱ': 'Math II',
        '数B': 'Math B',
        '数C': 'Math C',
        '数①': 'Math ①',
        '数②': 'Math ②',
        '数S': 'Math S',
        'EEC': 'EEC',
        'IEC': 'IEC',
        'EECI': 'EEC I',
        'EECII': 'EEC II',
        'EECIII': 'EEC III',
        'IECI': 'IEC I',
        'IECII': 'IEC II',
        'IECIII': 'IEC III',
        '1年': 'Grade 1',
        '2年': 'Grade 2',
        '3年': 'Grade 3',
        '1A': '1A',
        '1B': '1B',
        '1C': '1C',
        '2A': '2A',
        '2B': '2B',
        '2C': '2C',
        '3A': '3A',
        '3B': '3B',
        '3C': '3C',
        '12': '12',
        '34': '34',
        '567': '567',
        '兼': '&',
        'vintage': 'Vintage',
        'vocabulary': 'Vocabulary',
        'test': 'Test',
        'Pre-test': 'Pre-test',
        'PostTest': 'Post Test',
        'PreTest': 'Pre Test'
    }

    def translate_line(line):
        line = line.strip()
        if not line:
            return line

        # Start with the original line
        translated = line

        # Apply translations in order of specificity (longer phrases first)
        sorted_translations = sorted(translations.items(), key=lambda x: len(x[0]), reverse=True)

        for jp, en in sorted_translations:
            translated = translated.replace(jp, en)

        # Clean up formatting
        translated = re.sub(r'_', ' ', translated)  # Replace underscores with spaces
        translated = re.sub(r'　', ' ', translated)  # Replace full-width spaces with regular spaces

        # Add proper spacing around specific patterns
        translated = re.sub(r'([a-zA-Z])([0-9])', r'\1 \2', translated)  # Letter followed by number
        translated = re.sub(r'([0-9])([a-zA-Z])', r'\1 \2', translated)  # Number followed by letter
        translated = re.sub(r'(Semester)(Midterm|Final)', r'\1 \2', translated)  # Semester + Midterm/Final
        translated = re.sub(r'(Mock Test)([0-9])', r'\1 \2', translated)  # Mock Test + number
        translated = re.sub(r'([0-9])(st|nd|rd|th)', r'\1\2', translated)  # Keep ordinals together
        translated = re.sub(r'(Grade)([0-9])', r'\1 \2', translated)  # Grade + number
        translated = re.sub(r'(Chapter)([0-9])', r'\1 \2', translated)  # Chapter + number
        translated = re.sub(r'(Test)([0-9])', r'\1 \2', translated)  # Test + number

        # Clean up multiple spaces
        translated = re.sub(r'\s+', ' ', translated)
        translated = translated.strip()

        return translated

    # Read the original file
    try:
        with open('examples/tests.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("Error: examples/tests.txt not found")
        return

    # Process all lines
    bilingual_lines = []
    for line in lines:
        original = line.strip()
        if original:
            translated = translate_line(original)
            bilingual_lines.append(f'{original}\t{translated}')
        else:
            bilingual_lines.append('')

    # Write to new file
    with open('examples/tests_bilingual_final.txt', 'w', encoding='utf-8') as f:
        for line in bilingual_lines:
            f.write(line + '\n')

    print('Final translation completed successfully!')
    print(f'Processed {len([l for l in bilingual_lines if l])} lines')
    print('Output file: examples/tests_bilingual_final.txt')

if __name__ == '__main__':
    main()