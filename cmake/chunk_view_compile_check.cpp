#include <ranges>
#include <vector>

int main() {
    std::vector<int> v = {1, 3, -1, 3};
    auto lazy_split_range = v | std::views::chunk(-2);
}
