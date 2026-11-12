# Phân bố lịch sử user và kích thước embedding

Ngày tính: 08/11/2026. Đếm trên Gold training đã pin, toàn bộ user train của từng ngành/fold.
Một tương tác là một distinct user–item pair; nhiều review cùng item chỉ tính một lần.

Validation: train trước 01/01/2021 UTC. Refit/test: train trước 01/01/2022 UTC.

## Train dùng validation

| Số item/user | All Beauty | Video Games | Industrial and Scientific |
|---|---:|---:|---:|
| 1 | 429,006 (93.81%) | 1,551,309 (73.18%) | 1,784,070 (79.21%) |
| 2 | 23,343 (5.10%) | 313,740 (14.80%) | 282,628 (12.55%) |
| 3 | 3,330 (0.73%) | 113,032 (5.33%) | 88,798 (3.94%) |
| 4 | 846 (0.18%) | 52,370 (2.47%) | 38,151 (1.69%) |
| 5 | 317 (0.07%) | 28,204 (1.33%) | 20,024 (0.89%) |
| >5 | 482 (0.11%) | 61,164 (2.89%) | 38,663 (1.72%) |
| Tổng user | 457,324 | 2,119,819 | 2,252,334 |

## Train refit dùng test

| Số item/user | All Beauty | Video Games | Industrial and Scientific |
|---|---:|---:|---:|
| 1 | 528,902 (93.45%) | 1,782,670 (73.42%) | 2,216,653 (78.90%) |
| 2 | 30,204 (5.34%) | 359,196 (14.79%) | 357,231 (12.71%) |
| 3 | 4,506 (0.80%) | 128,112 (5.28%) | 112,285 (4.00%) |
| 4 | 1,163 (0.21%) | 59,040 (2.43%) | 48,381 (1.72%) |
| 5 | 422 (0.07%) | 31,422 (1.29%) | 25,304 (0.90%) |
| >5 | 780 (0.14%) | 67,520 (2.78%) | 49,710 (1.77%) |
| Tổng user | 565,977 | 2,427,960 | 2,809,564 |

## Giả định giảm bảng user embedding

Chưa sửa notebook. Mô phỏng chỉ cấp embedding riêng cho user có ít nhất k items; user còn lại dùng một hàng chung.
Giữ toàn bộ item mapping, số chiều 32 và kiến trúc Dense hiện tại. Tỷ lệ dưới đây là giảm tham số, không phải đo tăng chất lượng.

| Fold | Ngành | k | User đủ lịch sử | % user | % train pairs của nhóm đó | Giảm tham số NCF | Giảm tham số D2 |
|---|---|---:|---:|---:|---:|---:|---:|
| validation | All_Beauty | 2 | 28,318 | 6.19% | 13.40% | 79.80% | 79.65% |
| validation | All_Beauty | 3 | 4,975 | 1.09% | 3.98% | 84.14% | 83.99% |
| validation | All_Beauty | 4 | 1,645 | 0.36% | 1.96% | 84.76% | 84.60% |
| validation | All_Beauty | 5 | 799 | 0.17% | 1.28% | 84.92% | 84.76% |
| validation | All_Beauty | 6 | 482 | 0.11% | 0.96% | 84.98% | 84.82% |
| test | All_Beauty | 2 | 37,075 | 6.55% | 14.53% | 79.17% | 79.05% |
| test | All_Beauty | 3 | 6,871 | 1.21% | 4.77% | 83.69% | 83.56% |
| test | All_Beauty | 4 | 2,365 | 0.42% | 2.59% | 84.36% | 84.24% |
| test | All_Beauty | 5 | 1,202 | 0.21% | 1.84% | 84.54% | 84.41% |
| test | All_Beauty | 6 | 780 | 0.14% | 1.50% | 84.60% | 84.47% |
| validation | Video_Games | 2 | 568,510 | 26.82% | 55.82% | 69.75% | 69.72% |
| validation | Video_Games | 3 | 254,770 | 12.02% | 37.95% | 83.86% | 83.82% |
| validation | Video_Games | 4 | 141,738 | 6.69% | 28.29% | 88.94% | 88.90% |
| validation | Video_Games | 5 | 89,368 | 4.22% | 22.33% | 91.29% | 91.25% |
| validation | Video_Games | 6 | 61,164 | 2.89% | 18.31% | 92.56% | 92.52% |
| test | Video_Games | 2 | 645,290 | 26.58% | 55.26% | 70.00% | 69.97% |
| test | Video_Games | 3 | 286,094 | 11.78% | 37.23% | 84.11% | 84.07% |
| test | Video_Games | 4 | 157,982 | 6.51% | 27.59% | 89.14% | 89.10% |
| test | Video_Games | 5 | 98,942 | 4.08% | 21.66% | 91.46% | 91.42% |
| test | Video_Games | 6 | 67,520 | 2.78% | 17.72% | 92.69% | 92.65% |
| validation | Industrial_and_Scientific | 2 | 468,264 | 20.79% | 44.89% | 69.58% | 69.55% |
| validation | Industrial_and_Scientific | 3 | 185,636 | 8.24% | 27.43% | 80.60% | 80.57% |
| validation | Industrial_and_Scientific | 4 | 96,838 | 4.30% | 19.21% | 84.06% | 84.03% |
| validation | Industrial_and_Scientific | 5 | 58,687 | 2.61% | 14.49% | 85.55% | 85.52% |
| validation | Industrial_and_Scientific | 6 | 38,663 | 1.72% | 11.40% | 86.33% | 86.30% |
| test | Industrial_and_Scientific | 2 | 592,911 | 21.10% | 45.55% | 69.84% | 69.82% |
| test | Industrial_and_Scientific | 3 | 235,680 | 8.39% | 28.01% | 81.10% | 81.07% |
| test | Industrial_and_Scientific | 4 | 123,395 | 4.39% | 19.73% | 84.63% | 84.61% |
| test | Industrial_and_Scientific | 5 | 75,014 | 2.67% | 14.98% | 86.16% | 86.13% |
| test | Industrial_and_Scientific | 6 | 49,710 | 1.77% | 11.87% | 86.95% | 86.93% |

Không chọn ngưỡng bằng lịch sử sau cutoff hoặc kết quả test. Dùng chung embedding làm mất phân biệt ID của nhóm ít lịch sử;
có thể cần nhánh content/history encoder hoặc chính sách fallback. Giữ all-query evaluation nếu đổi cách biểu diễn user.

Giảm dim 32 → 16 làm gần một nửa phần tham số embeddings; FP16/quantization giảm byte lưu, không làm giảm số tham số.

Chi tiết số nguyên, provenance và checks nằm trong user_history_counts.json cùng thư mục.
