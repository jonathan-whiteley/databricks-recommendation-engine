import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface User {
  user_id: string;
  primary_store?: string | null;
  total_orders?: number | null;
}

interface UserSearchProps {
  users: User[];
  selectedUser: string | null;
  onUserSelect: (userId: string) => void;
}

export function UserSearch({ users, selectedUser, onUserSelect }: UserSearchProps) {
  return (
    <Select value={selectedUser ?? ""} onValueChange={onUserSelect}>
      <SelectTrigger className="w-72">
        <SelectValue placeholder="Select a user ID..." />
      </SelectTrigger>
      <SelectContent>
        {users.map((u) => (
          <SelectItem key={u.user_id} value={u.user_id}>
            <div className="flex items-center gap-2">
              <span>{u.user_id}</span>
              {u.primary_store && (
                <span className="text-xs text-muted-foreground">
                  {u.primary_store}
                </span>
              )}
            </div>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
