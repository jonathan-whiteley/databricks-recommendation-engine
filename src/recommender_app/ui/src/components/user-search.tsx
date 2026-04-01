import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface UserSearchProps {
  users: { user_id: string }[];
  selectedUser: string | null;
  onUserSelect: (userId: string) => void;
}

export function UserSearch({ users, selectedUser, onUserSelect }: UserSearchProps) {
  return (
    <Select value={selectedUser ?? ""} onValueChange={onUserSelect}>
      <SelectTrigger className="w-64">
        <SelectValue placeholder="Select a user ID..." />
      </SelectTrigger>
      <SelectContent>
        {users.map((u) => (
          <SelectItem key={u.user_id} value={u.user_id}>
            {u.user_id}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
