# 📬 message-queue
Asynchronous (buffering) MPI message queue implementation 📬

This library provides transparent buffering and asynchronous lazy messaging for MPI.

This has been developed as part of our work on [distributed triangle counting](https://github.com/niklas-uhl/katric).

If you use this code in the context of an academic publication, we kindly ask you to cite [the corresponding paper](https://doi.org/10.1109/IPDPS54959.2023.00076):

```bibtex
@inproceedings{sanders2023
  author       = {Peter Sanders and
                  Tim Niklas Uhl},
  title        = {Engineering a Distributed-Memory Triangle Counting Algorithm},
  booktitle    = {{IEEE} International Parallel and Distributed Processing Symposium,
                  {IPDPS} 2023, St. Petersburg, FL, USA, May 15-19, 2023},
  pages        = {702--712},
  publisher    = {{IEEE}},
  year         = {2023},
  url          = {https://doi.org/10.1109/IPDPS54959.2023.00076},
  doi          = {10.1109/IPDPS54959.2023.00076},
}
```

You can also find a [freely accessible postprint in the arXiv](https://arxiv.org/abs/2302.11443).

## Usage
This library is header only. You need a C++23 ready compiler, since it relies heavily on C++ concepts and the ranges library.

To use it in your project, include this repo using `FetchContent`, as `git submodule` or your preferred way of CMake dependency management and (if needed) include it as subdirectory. You can link against it using

``` cmake
# if using git submodules
add_subdirectory(path/to/submodule)

# if using FetchContent
FetchContent_Declare(message-queue
  GIT_REPOSITORY https://github.com/niklas-uhl/message-queue
  GIT_TAG main
)
FetchContent_MakeAvailable(message-queue)

# link against the target
target_link_libraries(<your-target> PRIVATE message-queue::message-queue)
```

For usage examples checks out the test directory.

## Caveats
This library relies heavily on C++ concepts and the ranges
library. Especially the latter is not fully supported by all compilers
claiming to support C++20. If you use a splitter function using
`std::ranges::split_view`, you should be aware that some older
compilers (< GCC 12 || < Clang 16) do not return a
`std::ranges::sized_range` when splitting a sized ranged (such as
message buffers), which is required for all messages used with the
message queue. This behavior has been described in
[P2210R2](https://wg21.link/P2210R2) and fixed in the current version
of the standard. `std::ranges::split_view` now returns subranges which
have the same iterator type as the underlying view, while the old
behavior has been renamed to `std::ranges::lazy_split_view`. If you
use a compiler without this defect fixed, you have to manually convert
the returned lazy split view to a sized split view. See the
implementation of `message_queue::SentinelSplitter` for an example.


