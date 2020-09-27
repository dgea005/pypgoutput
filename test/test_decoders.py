from pypgoutput import decoders


TEST_DIR = "files"

# files have been produced by get_replication_records.py
# file name schem: {lsn}_{message_type.lower()}_{file_name}
test_files = [
    "0_r_90fbc783-4017-4223-b104-504ff1f444f1",
    "0_r_a0ad09a5-119b-4162-8a64-47613c1c9f75",
    "0_r_b9b1606a-9a9e-44a0-bbdf-c50959ab86fd",
    "23704624_b_2f49c54c-a06a-42a1-8b0d-ad999985dfc1",
    "23716712_t_1986ec26-27ad-4ca7-9c87-7ed45c62ddf6",
    "23717000_c_66f40d53-2d34-47e3-b766-1e42905459e2",
    "23717056_b_734719fc-2786-4710-a4e9-a2cdb17917da",
    "23717056_i_c88d73f7-6ac1-4101-afeb-93fe7d70f0be",
    "23717336_b_30f5367b-8aa5-4038-bec8-58867486203c",
    "23717336_c_99f92407-2cde-4b73-a7e3-40eaa631d295",
    "23717336_i_443e6a03-c0d6-43c6-b652-0e06588cbdfe",
    "23717520_c_aa8edd8d-6842-483a-a8dc-f7a58c0a1a0d",
    "23717576_b_0d600c48-54d0-4831-867d-14730d55989d",
    "23717576_u_e9914102-86ea-4621-a7f9-7971234e3e55",
    "23717712_b_ade93d5f-2c83-4a49-aeab-d1454c2db728",
    "23717712_c_d7d16dcf-3714-4e3b-807b-d3d13fd0249e",
    "23717712_d_395ef829-32ca-4484-9bdd-949fb69ef31e",
    "23717824_b_d1d1e146-8d2d-4d44-bd06-8e037f27993f",
    "23717824_c_99c5817a-7a59-4f00-9ad8-e7758ba08214",
    "23719224_t_dbabb219-caf2-49e8-b6c7-9950d55228e8",
    "23719512_c_bc7a7f17-66ea-4fcf-9eba-a5aae5216753",
]


def test_opening_binary_files():

    for test_file in test_files:
        with open(f"{TEST_DIR}/{test_file}", 'rb') as f:
            test_message = f.read()
        message = decoders.decode_message(test_message)
        
