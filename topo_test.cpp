#include <mpi.h>
#include <cassert>
#include <iostream>
#include <string>

int main() {
    MPI_Session session = MPI_SESSION_NULL;
    MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_ARE_FATAL, &session);
    MPI_Group group = MPI_GROUP_NULL;
    MPI_Group_from_session_pset(session, "mpi://WORLD", &group);
    MPI_Comm comm = MPI_COMM_NULL;
    MPI_Comm_create_from_group(group, "edu.kit.message-queue", MPI_INFO_NULL, MPI_ERRORS_ARE_FATAL, &comm);
    MPI_Group_free(&group);
    int rank = 0;
    MPI_Comm_rank(comm, &rank);
    MPI_Info info = MPI_INFO_NULL;
    MPI_Info_create(&info);
    MPI_Comm new_comm = MPI_COMM_NULL;
    MPI_Comm_split_type(comm, MPI_COMM_TYPE_HW_UNGUIDED, rank, info, &new_comm);

    int buflen = 0;
    int flag = 0;
    std::string value;
    MPI_Info_get_string(info, "mpi_hw_resource_type", &buflen, nullptr, &flag);
    if (flag != 0) {
        value.resize(buflen - 1);
        MPI_Info_get_string(info, "mpi_hw_resource_type", &buflen, value.data(), &flag);
    }
    if (rank == 0) {
        std::cout << "mpi_hw_resource_type: " << value << "\n";
    }

    MPI_Info_free(&info);
    if (new_comm != MPI_COMM_NULL) {
      MPI_Comm_rank(new_comm, &rank);
      int size = 0;
      MPI_Comm_size(new_comm, &size);
      std::cout << "rank: " << rank << " of " << size << "\n";
      MPI_Comm_free(&new_comm);
    }
    MPI_Comm_free(&comm);

    MPI_Session_finalize(&session);
}
