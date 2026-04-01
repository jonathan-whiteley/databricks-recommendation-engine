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
    <div className="flex items-center gap-3 bg-[#f6f3f2] px-4 py-2 rounded-full cursor-pointer hover:bg-[#eae7e7] transition-colors">
      <span className="material-symbols-outlined text-brand">person_check</span>
      <Select value={selectedUser ?? ""} onValueChange={onUserSelect}>
        <SelectTrigger className="border-none bg-transparent shadow-none p-0 h-auto min-w-[140px] font-bold font-[Plus_Jakarta_Sans] text-sm focus:ring-0">
          <SelectValue placeholder="Select user..." />
        </SelectTrigger>
        <SelectContent>
          {users.map((u) => (
            <SelectItem key={u.user_id} value={u.user_id}>
              <div className="flex items-center gap-2">
                <span>{u.user_id}</span>
                {u.primary_store && (
                  <span className="text-xs text-stone-400">
                    {u.primary_store}
                  </span>
                )}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
