import sys
from collections import defaultdict


def validate_record(record, delimiter, seen_ids):
    errors = []
    parts = record.strip().split(delimiter)

    if len(parts) != 3:
        errors.append(f"Ожидается 3 поля, получено {len(parts)}")
        return False, errors

    id_str, name, value_str = parts

    id_errors = []
    if not id_str.strip():
        id_errors.append("Пустой идентификатор")
    else:
        try:
            id_num = int(id_str)
            if id_num in seen_ids:
                id_errors.append(f"Дублирующийся идентификатор")
        except ValueError:
            id_errors.append(f"Некорректный формат идентификатора")

    name_errors = []
    if not name.strip():
        name_errors.append("Пустое название")

    value_errors = []
    if not value_str.strip():
        value_errors.append("Пустое значение")
    else:
        try:
            value = float(value_str)
            if value < 0:
                value_errors.append("Отрицательное значение")
        except ValueError:
            value_errors.append("Некорректное числовое значение")

    all_errors = []
    if id_errors:
        all_errors.extend([f"ID: {error}" for error in id_errors])
    if name_errors:
        all_errors.extend([f"Name: {error}" for error in name_errors])
    if value_errors:
        all_errors.extend([f"Value: {error}" for error in value_errors])

    if all_errors:
        return False, all_errors

    try:
        id_num = int(id_str)
        seen_ids.add(id_num)
    except ValueError:
        return False, ["Внутренняя ошибка: не удалось преобразовать ID"]

    return True, []


def process_file(input_path, delimiter=',', output_path='report.txt'):
    total_records = 0
    valid_records = []
    invalid_records = []
    error_counts = defaultdict(int)
    seen_ids = set()

    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, start=1):
                total_records += 1
                is_valid, errors = validate_record(line, delimiter, seen_ids)

                if is_valid:
                    parts = line.strip().split(delimiter)
                    try:
                        id_num = int(parts[0])
                        name = parts[1]
                        value = float(parts[2])
                        valid_records.append({
                            'id': id_num,
                            'name': name,
                            'value': value,
                            'original': line.strip()
                        })
                    except (ValueError, IndexError):
                        invalid_records.append((line_num, line.strip(), ["Внутренняя ошибка обработки"]))
                        error_counts["Внутренняя ошибка обработки"] += 1
                else:
                    invalid_records.append((line_num, line.strip(), errors))
                    for error in errors:
                        error_counts[error] += 1
    except FileNotFoundError:
        print(f"Ошибка: файл '{input_path}' не найден.")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    if valid_records:
        values = [record['value'] for record in valid_records]
        avg_value = sum(values) / len(values)
        max_value = max(values)
        min_value = min(values)

        ids = [record['id'] for record in valid_records]
        min_id = min(ids)
        max_id = max(ids)
        unique_ids_count = len(set(ids))
    else:
        avg_value = max_value = min_value = 0
        min_id = max_id = unique_ids_count = 0

    report_lines = [
        "=" * 70,
        "ОТЧЁТ ОБ ОБРАБОТКЕ ДАННЫХ",
        "=" * 70,
        f"Общее количество записей: {total_records}",
        f"Корректных записей: {len(valid_records)}",
        f"Некорректных записей: {len(invalid_records)}",
        f"Уникальных идентификаторов: {unique_ids_count}",
        f"Общее количество ошибок: {sum(error_counts.values())}",
        "\nАгрегированная статистика (по корректным записям):",
        f"  Диапазон идентификаторов: {min_id} - {max_id}",
        f"  Среднее значение value: {avg_value:.2f}",
        f"  Максимальное значение value: {max_value:.2f}",
        f"  Минимальное значение value: {min_value:.2f}",
    ]

    report_lines.append("\nПолный список ошибок (по типам):")
    for error, count in sorted(error_counts.items()):
        report_lines.append(f"  - {error}: {count}")

    report_lines.append("\nДетализация некорректных записей:")
    if invalid_records:
        for line_num, record, errors in invalid_records:
            if len(errors) == 1:
                report_lines.append(f"  Строка {line_num}: {record}")
                report_lines.append(f"      Ошибка: {errors[0]}")
            else:
                report_lines.append(f"  Строка {line_num}: {record}")
                report_lines.append(f"      Обнаружено {len(errors)} ошибок:")
                for i, error in enumerate(errors, 1):
                    report_lines.append(f"        {i}. {error}")
            report_lines.append("")
    else:
        report_lines.append("  Некорректных записей не найдено.")

    report_lines.append("\nКорректные записи:")
    if valid_records:
        for record in sorted(valid_records, key=lambda x: x['id']):
            report_lines.append(
                f"  ID: {record['id']:4} | Название: {record['name']:15} | Value: {record['value']:7.2f}")
    else:
        report_lines.append("  Корректных записей не найдено.")

    report_text = "\n".join(report_lines)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"Отчёт сохранён в файл: {output_path}")
    except Exception as e:
        print(f"Ошибка при сохранении отчёта: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python data_processor.py <input_file> [delimiter] [output_file]")
        print("Пример: python data_processor.py data.txt ',' report.txt")
        print("\nФормат данных (3 поля): id,name,value")
        print("  - id: уникальный целочисленный идентификатор (обязательное поле)")
        print("  - name: непустое название (обязательное поле)")
        print("  - value: неотрицательное число (обязательное поле)")
        print("\nПрограмма обнаруживает ВСЕ ошибки в каждой строке.")
        sys.exit(1)

    input_file = sys.argv[1]
    delimiter = sys.argv[2] if len(sys.argv) > 2 else ','
    output_file = sys.argv[3] if len(sys.argv) > 3 else 'report.txt'

    process_file(input_file, delimiter, output_file)