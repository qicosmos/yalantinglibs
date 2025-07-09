#include <torch/torch.h>
#include <iostream>

int main() {
    // std::cout << "LibTorch version: " << torch::get_version() << std::endl;
    int i = 1;
    char c = 1 + '0';
    torch::Tensor tensor = torch::rand({2, 3});
    std::cout << "A random tensor:\n" << tensor << std::endl;
    return 0;
}
