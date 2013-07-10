function tear_down_space(space_no)
    box.space[box.unpack("i", space_no)]:truncate()
end

